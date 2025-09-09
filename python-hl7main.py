import socket
import threading
import hl7
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
            self.socket.bind((self.host, self.port))
            self.socket.listen(5)
            self.running = True
            
            logging.info(f"HL7 Server started on {self.host}:{self.port}")
            
            while self.running:
                try:
                    client_socket, address = self.socket.accept()
                    logging.info(f"Connection from {address}")
                    
                    # Handle each client in a separate thread
                    client_thread = threading.Thread(
                        target=self.handle_client, 
                        args=(client_socket, address)
                    )
                    client_thread.start()
                    
                except socket.error as e:
                    if self.running:
                        logging.error(f"Socket error: {e}")
                        
        except Exception as e:
            logging.error(f"Server error: {e}")
        finally:
            if self.socket:
                self.socket.close()
    
    def handle_client(self, client_socket, address):
        """Handle individual client connections"""
        try:
            buffer = b""
            
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break
                    
                buffer += data
                
                # HL7 messages typically end with \x1c\x0d (FS + CR)
                while b'\x1c\x0d' in buffer:
                    message, buffer = buffer.split(b'\x1c\x0d', 1)
                    
                    # Remove start character if present
                    if message.startswith(b'\x0b'):
                        message = message[1:]
                    
                    # Process the HL7 message
                    self.process_hl7_message(message.decode('utf-8'), address)
                    
                    # Send ACK response
                    ack_response = self.create_ack_response()
                    client_socket.send(ack_response.encode('utf-8'))
                    
        except Exception as e:
            logging.error(f"Error handling client {address}: {e}")
        finally:
            client_socket.close()
            logging.info(f"Connection closed for {address}")
    
    def process_hl7_message(self, message_text, address):
        """Process received HL7 message"""
        try:
            # Parse HL7 message
            message = hl7.parse(message_text)
            
            logging.info(f"Received HL7 message from {address}")
            logging.info(f"Message Type: {message['MSH'][9][0]}")
            
            # Handle different message types
            message_type = str(message['MSH'][9][0])
            
            if message_type == 'ORU':  # Observation Result
                self.handle_oru_message(message)
            elif message_type == 'QRY':  # Query
                self.handle_qry_message(message)
            else:
                logging.info(f"Unhandled message type: {message_type}")
                
            # Save raw message to file
            self.save_message_to_file(message_text)
            
        except Exception as e:
            logging.error(f"Error processing HL7 message: {e}")
            logging.info(f"Raw message: {message_text}")
    
    def handle_oru_message(self, message):
        """Handle ORU (Observation Result) messages from Swelab Alfa"""
        try:
            patient_id = str(message['PID'][3][0])
            patient_name = str(message['PID'][5][0])
            
            logging.info(f"Patient ID: {patient_id}")
            logging.info(f"Patient Name: {patient_name}")
            
            # Process OBX segments (observation results)
            for segment in message:
                if segment[0] == 'OBX':
                    test_code = str(segment[3][0])
                    test_name = str(segment[3][1])
                    result_value = str(segment[5][0])
                    units = str(segment[6][0]) if len(segment[6]) > 0 else ""
                    reference_range = str(segment[7][0]) if len(segment[7]) > 0 else ""
                    
                    logging.info(f"Test: {test_name} ({test_code})")
                    logging.info(f"Result: {result_value} {units}")
                    logging.info(f"Reference Range: {reference_range}")
                    
                    # Store results in database or process as needed
                    self.store_test_result(patient_id, test_code, test_name, 
                                         result_value, units, reference_range)
                    
        except Exception as e:
            logging.error(f"Error handling ORU message: {e}")
    
    def handle_qry_message(self, message):
        """Handle QRY (Query) messages"""
        logging.info("Received query message")
        # Implement query handling logic here
    
    def store_test_result(self, patient_id, test_code, test_name, 
                         result_value, units, reference_range):
        """Store test results (implement database storage here)"""
        result = {
            'timestamp': datetime.now(),
            'patient_id': patient_id,
            'test_code': test_code,
            'test_name': test_name,
            'result_value': result_value,
            'units': units,
            'reference_range': reference_range
        }
        
        # For now, just log the result
        logging.info(f"Stored result: {result}")
        
        # TODO: Implement database storage
        # self.save_to_database(result)
    
    def save_message_to_file(self, message_text):
        """Save raw HL7 message to file for debugging"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"hl7_message_{timestamp}.txt"
        
        try:
            with open(filename, 'w') as f:
                f.write(message_text)
            logging.info(f"Message saved to {filename}")
        except Exception as e:
            logging.error(f"Error saving message to file: {e}")
    
    def create_ack_response(self):
        """Create ACK response for received messages"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        ack = f"MSH|^~\\&|RECEIVER|HOSPITAL|SWELAB|LAB|{timestamp}||ACK|{timestamp}|P|2.4\r"
        ack += f"MSA|AA|{timestamp}|Message accepted\r"
        
        return f"\x0b{ack}\x1c\x0d"
    
    def stop_server(self):
        """Stop the HL7 server"""
        self.running = False
        if self.socket:
            self.socket.close()
        logging.info("HL7 Server stopped")

    
import subprocess

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

def main():
    """Main function to start the HL7 server"""
    # Verify network connectivity first
    if not verify_network_setup():
        logging.warning("Network connectivity issue detected, but continuing...")
    
    # Configuration for direct ethernet connection to Swelab
    HOST = '0.0.0.0'  # Listen on all interfaces
    PORT = 4001       # Match the client port from Swelab machine
    
    # Create and start server
    server = HL7Server(HOST, PORT)
    
    try:
        server.start_server()
    except KeyboardInterrupt:
        logging.info("Shutting down server...")
        server.stop_server()

if __name__ == "__main__":
    main()