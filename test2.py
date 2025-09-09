import socket
import threading
import hl7
from datetime import datetime
import logging
import subprocess
import socket
import time
import csv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_mac_ip_on_swelab_network():
    """Get your Mac's IP address that can reach the Swelab"""
    try:
        # Create a socket to connect to Swelab and see which local IP is used
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("169.254.51.192", 80))
        local_ip = sock.getsockname()[0]
        sock.close()
        logging.info(f"Your Mac's IP that can reach Swelab: {local_ip}")
        return local_ip
    except Exception as e:
        logging.error(f"Could not determine local IP: {e}")
        return None

class HL7Server:
    def __init__(self, host='localhost', port=8080):
        self.host = host
        self.port = port
        self.socket = None
        self.running = False
        
    def start_server(self):
        """Start the HL7 server to listen for incoming messages"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.settimeout(2.0)  # Shorter timeout for more frequent status updates
            self.socket.bind((self.host, self.port))
            self.socket.listen(5)
            self.running = True
            
            logging.info(f"HL7 Server STARTED and LISTENING on {self.host}:{self.port}")
            logging.info("=" * 50)
            logging.info("WAITING FOR SWELAB CONNECTION...")
            logging.info("=" * 50)
            
            last_status_time = time.time()
            
            while self.running:
                try:
                    # Show status every 10 seconds
                    current_time = time.time()
                    if current_time - last_status_time > 10:
                        logging.info("Still waiting for Swelab connection...")
                        last_status_time = current_time
                    
                    client_socket, address = self.socket.accept()
                    
                    logging.info("*" * 60)
                    logging.info(f"*** SWELAB CONNECTED FROM {address} ***")
                    logging.info("*" * 60)
                    
                    # Handle each client in a separate thread
                    client_thread = threading.Thread(
                        target=self.handle_client, 
                        args=(client_socket, address)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                    
                except socket.timeout:
                    continue
                except socket.error as e:
                    if self.running:
                        logging.error(f"Socket error: {e}")
                        
        except Exception as e:
            logging.error(f"Server error: {e}")
        finally:
            self.stop_server()
    
    def handle_client(self, client_socket, address):
        """Handle individual client connections"""
        try:
            buffer = b""
            
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break
                    
                logging.info(f"Received {len(data)} bytes from {address}")
                buffer += data
                
                # HL7 messages end with \x1c\x0d (FS + CR)
                while b'\x1c\x0d' in buffer:
                    message, buffer = buffer.split(b'\x1c\x0d', 1)
                    
                    # Remove start character if present
                    if message.startswith(b'\x0b'):
                        message = message[1:]
                    
                    # Convert to string
                    message_text = message.decode('utf-8', errors='ignore')
                    
                    logging.info("*" * 60)
                    logging.info("RECEIVED COMPLETE HL7 MESSAGE")
                    logging.info("*" * 60)
                    
                    # Process the complete HL7 message
                    self.process_complete_hl7_message(message_text, address)
                    
                    # Create and send ACK response
                    ack_response = self.create_ack_response(message_text)
                    client_socket.send(ack_response.encode('utf-8'))
                    
                    logging.info("*" * 60)
                    logging.info("ACK RESPONSE SENT SUCCESSFULLY")
                    logging.info("*" * 60)
                    
        except Exception as e:
            logging.error(f"Error handling client {address}: {e}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
        finally:
            try:
                client_socket.close()
            except:
                pass
            logging.info(f"Connection closed for {address}")
    
    def process_complete_hl7_message(self, complete_message, address):
        """Process complete HL7 message with all segments"""
        try:
            logging.info(f"Processing complete HL7 message from {address}")
            logging.info("=" * 60)
            
            # Split message into segments
            segments = complete_message.split('\r')
            segments = [seg.strip() for seg in segments if seg.strip()]
            
            logging.info(f"Total segments received: {len(segments)}")
            logging.info("=" * 60)
            
            # Process each segment
            patient_data = {}
            lab_results = []
            
            for i, segment in enumerate(segments):
                if not segment:
                    continue
                    
                segment_type = segment[:3]
                logging.info(f"Segment {i+1}: {segment_type} - {segment}")
                
                if segment_type == 'MSH':
                    # Message Header
                    fields = segment.split('|')
                    if len(fields) > 10:
                        patient_data['message_control_id'] = fields[10]
                        patient_data['timestamp'] = fields[7] if len(fields) > 7 else ''
                        
                elif segment_type == 'OBR':
                    # Observation Request
                    fields = segment.split('|')
                    if len(fields) > 4:
                        patient_data['patient_info'] = fields[4]
                        patient_data['specimen_id'] = fields[3] if len(fields) > 3 else ''
                        
                elif segment_type == 'OBX':
                    # Observation Result
                    fields = segment.split('|')
                    if len(fields) > 5:
                        test_result = {
                            'test_code': fields[3] if len(fields) > 3 else '',
                            'test_value': fields[5] if len(fields) > 5 else '',
                            'units': fields[6] if len(fields) > 6 else '',
                            'reference_range': fields[7] if len(fields) > 7 else '',
                            'status': fields[11] if len(fields) > 11 else ''
                        }
                        lab_results.append(test_result)
            
            # Display processed results
            logging.info("=" * 60)
            logging.info("PROCESSED LAB RESULTS:")
            logging.info("=" * 60)
            
            if patient_data:
                logging.info(f"Patient: {patient_data.get('patient_info', 'Unknown')}")
                logging.info(f"Specimen ID: {patient_data.get('specimen_id', 'Unknown')}")
                logging.info(f"Timestamp: {patient_data.get('timestamp', 'Unknown')}")
                logging.info("-" * 40)
            
            for result in lab_results:
                if result['test_code'] and result['test_value']:
                    logging.info(f"{result['test_code']}: {result['test_value']} {result['units']} (Ref: {result['reference_range']})")
            
            logging.info("=" * 60)
            
            # Save to file
            self.save_lab_results_to_file(patient_data, lab_results)
            
        except Exception as e:
            logging.error(f"Error processing complete HL7 message: {e}")
            logging.error(f"Message content: {complete_message}")
    
 

    def save_lab_results_to_file(self, patient_data, lab_results):
        """Save lab results to a CSV file with patient-based filename"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Get patient data for filename
        patient_name = patient_data.get('patient_info', 'Unknown_Patient')
        specimen_id = patient_data.get('specimen_id', 'Unknown_Specimen')
        
        # Clean filename by replacing spaces and special characters
        clean_patient_name = patient_name.replace(' ', '_').replace('/', '_').replace('\\', '_').replace(':', '_')
        clean_specimen_id = specimen_id.replace(' ', '_').replace('/', '_').replace('\\', '_').replace(':', '_')
        
        # Create filename
        filename = f"{clean_patient_name}_{clean_specimen_id}_{timestamp}.csv"
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write patient information at the top
                writer.writerow(['Patient Information'])
                writer.writerow(['Patient Name:', patient_name])
                writer.writerow(['Specimen ID:', specimen_id])
                writer.writerow(['Timestamp:', datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
                writer.writerow([])  # Empty row for separation
                
                # Define CSV headers for lab results
                fieldnames = ['Test_Code', 'Test_Name', 'Result_Value', 'Units', 'Reference_Range', 'Status']
                writer.writerow(fieldnames)
                
                # Write lab results
                for result in lab_results:
                    if result['test_code'] and result['test_value']:
                        writer.writerow([
                            result['test_code'],
                            self.get_test_name(result['test_code']),
                            result['test_value'],
                            result['units'],
                            result['reference_range'],
                            result['status']
                        ])
                
            logging.info(f"Lab results saved to CSV: {filename}")
            logging.info(f"Total tests saved: {len([r for r in lab_results if r['test_code'] and r['test_value']])}")
            
        except Exception as e:
            logging.error(f"Error saving lab results to CSV: {e}")

    def get_test_name(self, test_code):
        """Convert test codes to readable names"""
        test_names = {
            'RBC': 'Red Blood Cells',
            'WBC': 'White Blood Cells',
            'HGB': 'Hemoglobin',
            'HCT': 'Hematocrit',
            'MCV': 'Mean Corpuscular Volume',
            'MCH': 'Mean Corpuscular Hemoglobin',
            'MCHC': 'Mean Corpuscular Hemoglobin Concentration',
            'PLT': 'Platelets',
            'MPV': 'Mean Platelet Volume',
            'RDWR': 'Red Cell Distribution Width',
            'RDWA': 'Red Cell Distribution Width Absolute',
            'PCT': 'Plateletcrit',
            'PDW': 'Platelet Distribution Width',
            'LPCR': 'Large Platelet Cell Ratio',
            'LYMA': 'Lymphocytes Absolute',
            'MIDA': 'Monocytes Absolute',
            'GRNA': 'Granulocytes Absolute',
            'LYMR': 'Lymphocytes Relative',
            'MIDR': 'Monocytes Relative',
            'GRNR': 'Granulocytes Relative'
        }
        
        return test_names.get(test_code, test_code)

    def create_ack_response(self, original_message=None):
        """Alternative ACK format with equipment details"""
        control_id = "BM_3"
        timestamp = "20250906121328"
        
        # Try different field arrangement
        ack = f"MSH|^~\\&|103868||BM850HL7MW||{timestamp}||ACK|{control_id}|P|2.7\r"
        ack += f"MSA|AA|{control_id}\r"
        
        return f"\x0b{ack}\x1c\x0d"

    def handle_client(self, client_socket, address):
        """Handle individual client connections with better error handling"""
        try:
            buffer = b""
            
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break
                    
                logging.info(f"Received {len(data)} bytes from {address}")
                buffer += data
                
                # HL7 messages end with \x1c\x0d (FS + CR)
                while b'\x1c\x0d' in buffer:
                    message, buffer = buffer.split(b'\x1c\x0d', 1)
                    
                    # Remove start character if present
                    if message.startswith(b'\x0b'):
                        message = message[1:]
                    
                    # Convert to string
                    message_text = message.decode('utf-8', errors='ignore')
                    
                    logging.info("=" * 60)
                    logging.info("RECEIVED COMPLETE HL7 MESSAGE")
                    logging.info("=" * 60)
                    logging.info(f"Raw message: {repr(message_text)}")
                    
                    # Process the complete HL7 message
                    self.process_complete_hl7_message(message_text, address)
                    
                    # Create and send ACK response
                    ack_response = self.create_ack_response(message_text)
                    
                    # Send ACK
                    bytes_sent = client_socket.send(ack_response.encode('utf-8'))
                    logging.info(f"ACK response sent: {bytes_sent} bytes")
                    logging.info(f"ACK content: {repr(ack_response)}")
                    
                    # Small delay to ensure ACK is processed
                    import time
                    time.sleep(0.1)
                    
        except Exception as e:
            logging.error(f"Error handling client {address}: {e}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
        finally:
            try:
                client_socket.close()
            except:
                pass
            logging.info(f"Connection closed for {address}")
    
    def stop_server(self):
        """Stop the HL7 server"""
        self.running = False
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
        logging.info("HL7 Server stopped")

def verify_network_setup():
    """Verify network connectivity to Swelab machine"""
    swelab_ip = "169.254.51.192"
    
    try:
        # Ping the Swelab machine
        result = subprocess.run(['ping', '-c', '3', swelab_ip], 
                              capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            logging.info(f"Successfully connected to Swelab at {swelab_ip}")
            return True
        else:
            logging.error(f"Cannot reach Swelab at {swelab_ip}")
            return False
    except Exception as e:
        logging.error(f"Network test failed: {e}")
        return False
    
def get_computer_name():
    """Get the computer's hostname"""
    try:
        hostname = socket.gethostname()
        logging.info(f"Your Mac's hostname: {hostname}")
        return hostname
    except Exception as e:
        logging.error(f"Could not get hostname: {e}")
        return None

def main():
    """Main function to start the HL7 server"""
    logging.info("=== Starting HL7 Server for Swelab Alfa ===")
    
    # Get both IP and hostname
    mac_ip = get_mac_ip_on_swelab_network()
    computer_name = get_computer_name()
    
    # Verify network connectivity first
    if not verify_network_setup():
        logging.warning("Network connectivity issue detected, but continuing...")
    
    # Configuration for direct ethernet connection to Swelab
    HOST = '0.0.0.0'     # Listen on ALL interfaces
    PORT = 4001          # Match the client port from Swelab machine
    
    logging.info("SWELAB CONFIGURATION OPTIONS:")
    if mac_ip:
        logging.info(f"Option 1 - Host IP: {mac_ip}")
    if computer_name:
        logging.info(f"Option 2 - Hostname: {computer_name}")
    logging.info("- Host Port: 4001")
    logging.info("- Mode: CLIENT (not server)")
    logging.info("- Protocol: TCP/IP")
    logging.info("- Message Format: HL7")
    logging.info("")
    
    # Create and start server
    server = HL7Server(HOST, PORT)
    
    try:
        server.start_server()
    except KeyboardInterrupt:   
        logging.info("Shutting down server...")
        server.stop_server()
    except Exception as e:
        logging.error(f"Failed to start server: {e}")

if __name__ == "__main__":
    main()