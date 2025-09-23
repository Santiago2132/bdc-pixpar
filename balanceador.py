import os
import threading
import time
import queue
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from werkzeug.serving import make_server
import xml.etree.ElementTree as ET
import requests
import socket
import subprocess
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Dict, Optional
import uuid
import json

def obtener_ip_real():
    """
    Obtiene la IP real de la máquina en la red local.
    """
    # Método 1: Conectar a un servidor externo (más confiable)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            if not ip.startswith("127."):
                return ip
    except:
        pass
    
    # Método 2: Usar ip route (Linux/Mac)
    try:
        result = subprocess.run(['ip', 'route', 'get', '8.8.8.8'], 
                              capture_output=True, text=True, timeout=3)
        match = re.search(r'src (\d+\.\d+\.\d+\.\d+)', result.stdout)
        if match:
            return match.group(1)
    except:
        pass
    
    # Método 3: Usar hostname -I (Linux)
    try:
        result = subprocess.run(['hostname', '-I'], 
                              capture_output=True, text=True, timeout=3)
        ips = result.stdout.strip().split()
        for ip in ips:
            if not ip.startswith("127.") and "." in ip:
                return ip
    except:
        pass
    
    # Fallback
    try:
        ip = socket.gethostbyname(socket.gethostname())
        if not ip.startswith("127."):
            return ip
    except:
        pass
    
    return "127.0.0.1"

app = Flask(__name__)
CORS(app)

@dataclass
class Nodo:
    ip: str
    puertos: List[int]
    servicios: Dict[str, str]
    capacidad_maxima: int
    capacidad_disponible: int
    ultimo_heartbeat: float
    activo: bool = True

@dataclass
class TareaProcesamiento:
    id: str
    xml_content: str
    prioridad: int
    timestamp: float
    tipo_servicio: str
    formato_salida: str = "JPEG"
    calidad: int = 85

class BalanceadorCargas:
    def __init__(self):
        self.nodos = {}  # ip -> Nodo
        self.cola_tareas = queue.PriorityQueue()
        self.resultados = {}  # task_id -> resultado
        self.lock = threading.Lock()
        self.timeout_nodo = 30  # segundos
        
        # Estadísticas
        self.stats = {
            "tareas_procesadas": 0,
            "tareas_pendientes": 0,
            "nodos_activos": 0,
            "tiempo_promedio": 0.0
        }
        
        # Iniciar hilos de monitoreo
        threading.Thread(target=self._monitor_nodos, daemon=True).start()
        threading.Thread(target=self._procesar_tareas, daemon=True).start()
    
    def registrar_nodo(self, data):
        """Registra un nuevo nodo o actualiza uno existente"""
        ip = data.get("ip")
        if not ip:
            return False
            
        with self.lock:
            nodo = Nodo(
                ip=ip,
                puertos=data.get("puertos", []),
                servicios=data.get("servicios", {}),
                capacidad_maxima=data.get("capacidad_maxima", 100000),
                capacidad_disponible=data.get("capacidad_maxima", 100000),
                ultimo_heartbeat=time.time(),
                activo=True
            )
            self.nodos[ip] = nodo
            print(f"✅ Nodo registrado: {ip} - Capacidad: {nodo.capacidad_maxima}")
            return True
    
    def _monitor_nodos(self):
        """Monitorea el estado de los nodos"""
        while True:
            try:
                tiempo_actual = time.time()
                nodos_inactivos = []
                
                with self.lock:
                    for ip, nodo in self.nodos.items():
                        if tiempo_actual - nodo.ultimo_heartbeat > self.timeout_nodo:
                            if nodo.activo:
                                print(f"⚠️  Nodo {ip} desconectado por timeout")
                                nodo.activo = False
                                nodos_inactivos.append(ip)
                        else:
                            # Actualizar capacidad del nodo
                            self._actualizar_capacidad_nodo(nodo)
                
                time.sleep(5)
            except Exception as e:
                print(f"Error monitoreando nodos: {e}")
    
    def _actualizar_capacidad_nodo(self, nodo):
        """Consulta la capacidad actual de un nodo"""
        try:
            url = f"http://{nodo.ip}:8003/estado"
            response = requests.get(url, timeout=3)
            if response.status_code == 200:
                estado = response.json()
                nodo.capacidad_disponible = estado.get("capacidad_disponible", 0)
                nodo.ultimo_heartbeat = time.time()
        except:
            pass
    
    def _procesar_tareas(self):
        """Procesa las tareas de la cola asignándolas a nodos disponibles"""
        while True:
            try:
                # Obtener tarea con mayor prioridad (número menor = mayor prioridad)
                prioridad, timestamp, tarea = self.cola_tareas.get(timeout=1)
                
                # Encontrar nodo disponible
                nodo_seleccionado = self._seleccionar_nodo(tarea)
                
                if nodo_seleccionado:
                    # Procesar tarea
                    threading.Thread(
                        target=self._ejecutar_tarea,
                        args=(tarea, nodo_seleccionado),
                        daemon=True
                    ).start()
                else:
                    # Reencolar si no hay nodos disponibles
                    self.cola_tareas.put((prioridad, timestamp, tarea))
                    time.sleep(0.5)
                    
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Error procesando tareas: {e}")
    
    def _seleccionar_nodo(self, tarea):
        """Selecciona el mejor nodo para procesar la tarea"""
        mejor_nodo = None
        mayor_capacidad = 0
        
        with self.lock:
            for nodo in self.nodos.values():
                if (nodo.activo and 
                    nodo.capacidad_disponible > mayor_capacidad and
                    self._nodo_soporta_servicio(nodo, tarea.tipo_servicio)):
                    mejor_nodo = nodo
                    mayor_capacidad = nodo.capacidad_disponible
        
        return mejor_nodo
    
    def _nodo_soporta_servicio(self, nodo, tipo_servicio):
        """Verifica si un nodo soporta el tipo de servicio requerido"""
        servicios_puerto = {
            "procesamiento_batch": 8001,
            "transformaciones_batch": 8002,
            "conversion_unica": 8004
        }
        puerto_requerido = servicios_puerto.get(tipo_servicio)
        return puerto_requerido in nodo.puertos
    
    def _ejecutar_tarea(self, tarea, nodo):
        """Ejecuta una tarea en un nodo específico"""
        try:
            inicio = time.time()
            
            # Determinar URL del servicio
            urls_servicio = {
                "procesamiento_batch": f"http://{nodo.ip}:8001/procesar",
                "transformaciones_batch": f"http://{nodo.ip}:8002/transformar",
                "conversion_unica": f"http://{nodo.ip}:8004/convertir"
            }
            
            url = urls_servicio.get(tarea.tipo_servicio)
            if not url:
                raise Exception(f"Tipo de servicio no soportado: {tarea.tipo_servicio}")
            
            # Parámetros adicionales para conversión
            params = {}
            if tarea.tipo_servicio == "conversion_unica":
                params = {
                    "formato": tarea.formato_salida,
                    "calidad": tarea.calidad
                }
            
            # Hacer petición al nodo
            response = requests.post(
                url,
                data=tarea.xml_content,
                headers={"Content-Type": "application/xml"},
                params=params,
                timeout=60
            )
            
            fin = time.time()
            tiempo_proceso = fin - inicio
            
            # Guardar resultado
            with self.lock:
                self.resultados[tarea.id] = {
                    "status": "completado" if response.status_code == 200 else "error",
                    "resultado": response.text,
                    "tiempo_proceso": tiempo_proceso,
                    "nodo_procesado": nodo.ip,
                    "timestamp": fin
                }
                
                # Actualizar estadísticas
                self.stats["tareas_procesadas"] += 1
                self.stats["tiempo_promedio"] = (
                    (self.stats["tiempo_promedio"] * (self.stats["tareas_procesadas"] - 1) + tiempo_proceso) /
                    self.stats["tareas_procesadas"]
                )
            
            print(f"✅ Tarea {tarea.id} completada en {tiempo_proceso:.2f}s por {nodo.ip}")
            
        except Exception as e:
            with self.lock:
                self.resultados[tarea.id] = {
                    "status": "error",
                    "error": str(e),
                    "timestamp": time.time()
                }
            print(f"❌ Error procesando tarea {tarea.id}: {e}")
    
    def agregar_tarea(self, xml_content, prioridad=5, tipo_servicio="procesamiento_batch", 
                     formato_salida="JPEG", calidad=85):
        """Agrega una nueva tarea a la cola"""
        task_id = str(uuid.uuid4())
        timestamp = time.time()
        
        tarea = TareaProcesamiento(
            id=task_id,
            xml_content=xml_content,
            prioridad=prioridad,
            timestamp=timestamp,
            tipo_servicio=tipo_servicio,
            formato_salida=formato_salida,
            calidad=calidad
        )
        
        self.cola_tareas.put((prioridad, timestamp, tarea))
        
        with self.lock:
            self.stats["tareas_pendientes"] = self.cola_tareas.qsize()
        
        return task_id
    
    def obtener_resultado(self, task_id):
        """Obtiene el resultado de una tarea"""
        with self.lock:
            return self.resultados.get(task_id)
    
    def obtener_estadisticas(self):
        """Obtiene estadísticas del balanceador"""
        with self.lock:
            self.stats["nodos_activos"] = sum(1 for n in self.nodos.values() if n.activo)
            self.stats["tareas_pendientes"] = self.cola_tareas.qsize()
            
            nodos_info = {}
            for ip, nodo in self.nodos.items():
                nodos_info[ip] = {
                    "activo": nodo.activo,
                    "capacidad_disponible": nodo.capacidad_disponible,
                    "capacidad_maxima": nodo.capacidad_maxima,
                    "ultimo_heartbeat": nodo.ultimo_heartbeat
                }
            
            return {
                **self.stats,
                "nodos": nodos_info
            }

# Instancia global del balanceador
balanceador = BalanceadorCargas()

@app.route('/api/nodos/registrar', methods=['POST'])
def registrar_nodo():
    """Endpoint para registrar nodos"""
    try:
        data = request.get_json()
        if balanceador.registrar_nodo(data):
            return jsonify({"status": "success", "message": "Nodo registrado correctamente"})
        else:
            return jsonify({"status": "error", "message": "Error registrando nodo"}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/procesar', methods=['POST'])
def procesar_imagenes():
    """Endpoint principal para procesar imágenes"""
    try:
        # Obtener parámetros
        prioridad = int(request.args.get('prioridad', 5))
        tipo_servicio = request.args.get('tipo', 'procesamiento_batch')
        formato_salida = request.args.get('formato', 'JPEG')
        calidad = int(request.args.get('calidad', 85))
        
        # Obtener contenido XML
        if request.content_type == 'application/xml' or request.content_type == 'text/xml':
            xml_content = request.data.decode('utf-8')
        else:
            xml_content = request.get_data(as_text=True)
        
        if not xml_content:
            return jsonify({"error": "No se recibió contenido XML"}), 400
        
        # Validar XML
        try:
            ET.fromstring(xml_content)
        except:
            return jsonify({"error": "XML malformado"}), 400
        
        # Agregar tarea
        task_id = balanceador.agregar_tarea(
            xml_content=xml_content,
            prioridad=prioridad,
            tipo_servicio=tipo_servicio,
            formato_salida=formato_salida,
            calidad=calidad
        )
        
        return jsonify({
            "status": "accepted",
            "task_id": task_id,
            "message": "Tarea agregada a la cola de procesamiento"
        })
        
    except Exception as e:
        return jsonify({"error": f"Error del servidor: {str(e)}"}), 500

@app.route('/api/resultado/<task_id>', methods=['GET'])
def obtener_resultado(task_id):
    """Obtiene el resultado de una tarea"""
    try:
        resultado = balanceador.obtener_resultado(task_id)
        
        if not resultado:
            return jsonify({"status": "not_found", "message": "Tarea no encontrada"}), 404
        
        if resultado["status"] == "completado":
            return Response(
                resultado["resultado"],
                mimetype='application/xml',
                headers={
                    "X-Processing-Time": str(resultado["tiempo_proceso"]),
                    "X-Processed-By": resultado["nodo_procesado"]
                }
            )
        else:
            return jsonify(resultado), 500
            
    except Exception as e:
        return jsonify({"error": f"Error del servidor: {str(e)}"}), 500

@app.route('/api/estadisticas', methods=['GET'])
def obtener_estadisticas():
    """Obtiene estadísticas del balanceador"""
    try:
        stats = balanceador.obtener_estadisticas()
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": f"Error del servidor: {str(e)}"}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check del balanceador"""
    return jsonify({
        "status": "healthy",
        "service": "Balanceador de Cargas - Procesamiento de Imágenes",
        "timestamp": time.time(),
        "nodos_activos": len([n for n in balanceador.nodos.values() if n.activo])
    })

@app.route('/api/nodos', methods=['GET'])
def listar_nodos():
    """Lista todos los nodos registrados"""
    try:
        nodos_info = {}
        with balanceador.lock:
            for ip, nodo in balanceador.nodos.items():
                nodos_info[ip] = {
                    "activo": nodo.activo,
                    "puertos": nodo.puertos,
                    "servicios": nodo.servicios,
                    "capacidad_disponible": nodo.capacidad_disponible,
                    "capacidad_maxima": nodo.capacidad_maxima,
                    "ultimo_heartbeat": nodo.ultimo_heartbeat
                }
        
        return jsonify(nodos_info)
    except Exception as e:
        return jsonify({"error": f"Error del servidor: {str(e)}"}), 500

def main():
    """Función principal"""
    print("🚀 Iniciando Balanceador de Cargas...")
    print("=" * 50)
    
    # Obtener IP real
    ip_local = obtener_ip_real()
    puerto = 5000
    
    print("📡 Servicios disponibles:")
    print(f"  • POST /api/nodos/registrar - Registrar nodos")
    print(f"  • POST /api/procesar - Procesar imágenes")
    print(f"  • GET /api/resultado/<task_id> - Obtener resultado")
    print(f"  • GET /api/estadisticas - Ver estadísticas")
    print(f"  • GET /api/nodos - Listar nodos")
    print(f"  • GET /api/health - Health check")
    
    print(f"\n⚡ Balanceador ejecutándose en: {ip_local}:{puerto}")
    print("⚡ Prioridades: 1=Muy Alta, 2=Alta, 3=Media, 4=Baja, 5=Muy Baja")
    print("⚡ Tipos de servicio: procesamiento_batch, transformaciones_batch, conversion_unica")
    print("⚡ Balanceador listo... (Ctrl+C para detener)")
    
    try:
        server = make_server(ip_local, puerto, app)
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo balanceador...")
        print("✅ Balanceador detenido")

if __name__ == "__main__":
    main()