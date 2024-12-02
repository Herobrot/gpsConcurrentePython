import serial
import re
import csv
import requests
import time
import logging
import threading
import queue
from typing import Optional, Tuple

# Configure logging
logging.basicConfig(
    filename='/home/jeshua/pythonface/env/gps_service.log',
    level=logging.INFO,
    format='%(asctime)s %(message)s'
)

# Compile regex pattern for GPGGA parsing
GPGGA_PATTERN = re.compile(r'^\$GPGGA,\d{6}\.\d{2},(\d{2})(\d{2}\.\d+),([NS]),(\d{3})(\d{2}\.\d+),([EW]),.*')

class GPSProcessor:
    def __init__(self, port: str = "/dev/ttyAMA0", baudrate: int = 9600, timeout: int = 1):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
    
        self.gps_data_queue = queue.Queue()
        self.csv_queue = queue.Queue()
        self.endpoint_queue = queue.Queue()
    
        self.stop_event = threading.Event()

        self.read_thread = None
        self.csv_thread = None
        self.endpoint_thread = None        

    def connect_serial(self) -> serial.Serial:
        try:
            print(f"[SERIAL] Conectándose al puerto {self.port}")
            serial_conn = serial.Serial(self.port, self.baudrate, timeout=self.timeout)
            print(f"[SERIAL] Conección correcta al puerto {self.port}")
            return serial_conn
        except serial.SerialException as e:
            print(f"Fallo al conectar al puerto: {e}")
            logging.error(f"Fallo al conectar al puerto: {e}")
            raise

    @staticmethod
    def parse_gpgga(data: str) -> Tuple[Optional[float], Optional[float]]:
        match = GPGGA_PATTERN.match(data)
        if match:
            lat_deg = int(match.group(1))
            lat_min = float(match.group(2))
            lat_dir = match.group(3)
            latitude = lat_deg + (lat_min / 60.0)
            if lat_dir == 'S':
                latitude = -latitude

            lon_deg = int(match.group(4))
            lon_min = float(match.group(5))
            lon_dir = match.group(6)
            longitude = lon_deg + (lon_min / 60.0)
            if lon_dir == 'W':
                longitude = -longitude

            print(f"[PARSE] Coordenadas parseadas: Lat {latitude}, Long {longitude}")
            return latitude, longitude
        print(f"[PARSE] No se pudo parsear")
        return None, None

    def read_gps_data(self):
        print("[THREAD] Hilo de lectura de datos del GPS iniciado")
        try:
            gps_serial = self.connect_serial()
            while not self.stop_event.is_set():
                try:
                    line = gps_serial.readline().decode('ascii', errors='replace')
                    print(f"[READ] Se recibió: {line.strip()}")
                    if line.startswith('$GPGGA'):
                        latitude, longitude = self.parse_gpgga(line)
                        if latitude is not None and longitude is not None:
                            self.gps_data_queue.put((latitude, longitude))
                            print(f"[QUEUE] Coordenadas agregadas: {latitude}, {longitude}")
                            logging.info(f"GPS Data: Lat {latitude}, Long {longitude}")
                except serial.SerialException:
                    print("[ERROR] Error serial, intentando reconectarse...")
                    logging.error("Serial communication error")
                    time.sleep(5)
                    gps_serial = self.connect_serial()
        except Exception as e:
            print(f"[ERROR] Error inesperado en read_gps_data: {e}")
            logging.error(f"Unexpected error in read_gps_data: {e}")
        finally:
            print("[THREAD] El hilo de lectura de GPS cerró.")
            gps_serial.close()

    def save_to_csv_worker(self):
        print("[THREAD] Hilo de escritura inicia")
        while not self.stop_event.is_set():
            try:
                lat, lon = self.csv_queue.get(timeout=1)
                with open('gps_data.csv', 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([lat, lon])
                    print(f"[PARSE] Coordenadas GUARDADAS: Lat {lat}, Long {lon}")
                logging.info(f"Saved to CSV: {lat}, {lon}")
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[ERROR] CSV Error de escritura: {e}")
                logging.error(f"CSV Error de escritura: {e}")
        print("[THREAD] CSV de escritura cerró")

    def send_to_endpoint_worker(self):
        #Actualizar url a la real cuando se actualice el git
        url = "https://sw972tdv-4000.usw3.devtunnels.ms/kits/66a33f426bc60fa809490bbb/gps"
        print("[THREAD] Hilo de Envío del endpoint inicio")
        while not self.stop_event.is_set():
            try:
                lat, lon = self.endpoint_queue.get(timeout=1)
                data = {"lat": lat, "long": lon}
                print(f"[ENDPOINT] Enviando la data {url}: {data}")
                response = requests.put(url, json=data)
                if response.status_code == 200:
                    print("[ENDPOINT] Fue enviado correctamente la data")
                    logging.info("[ENDPOINT] Fue enviado correctamente la data")
                else:
                    print(f"[ENDPOINT] Falló al enviar la data. Status code: {response.status_code}")
                    logging.warning(f"Failed to send data. Status code: {response.status_code}")
                time.sleep(10)
            except queue.Empty:
                continue
            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed: {e}")

    def process_gps_data(self):
        print("[THREAD] Hilo de procesamiento del GPS iniciado")
        while not self.stop_event.is_set():
            try:
                latitude, longitude = self.gps_data_queue.get(timeout=1)
                self.csv_queue.put((latitude, longitude))
                self.endpoint_queue.put((latitude, longitude))
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Data processing error: {e}")

    def start(self):
        try:
            self.read_thread = threading.Thread(target=self.read_gps_data)
            self.process_thread = threading.Thread(target=self.process_gps_data)
            self.csv_thread = threading.Thread(target=self.save_to_csv_worker)
            self.endpoint_thread = threading.Thread(target=self.send_to_endpoint_worker)

            threads = [
                self.read_thread,
                self.process_thread,
                self.csv_thread,
                self.endpoint_thread
            ]
            for thread in threads:
                thread.daemon = True
                thread.start()

            for thread in threads:
                thread.join()

        except Exception as e:
            logging.error(f"Error starting threads: {e}")
        finally:
            self.stop_event.set()

    def stop(self):
        self.stop_event.set()

def main():
    gps_processor = GPSProcessor()
    try:
        gps_processor.start()
    except KeyboardInterrupt:
        print("GPS processing stopped.")
        gps_processor.stop()

if __name__ == "__main__":
    main()
