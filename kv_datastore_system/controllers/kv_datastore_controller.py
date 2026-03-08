"""
This Module implements the controller or handler
that receives the request through API endpoints
Calls a kv_datastore_service according to the functionality
requested
"""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from http import HTTPStatus
from urllib.parse import urlparse, parse_qs
import argparse
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)

from kv_datastore_system.services.kv_datastore_service import KVDataStoreService

class KVDataStoreController(BaseHTTPRequestHandler):
    """
    Class that handles the requests to the KV Data Store
    """
    kv_datastore_service  = None

    def _send_json(self, data, status=200):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        body = json.loads(self.rfile.read(content_length))
        
        if self.path == '/put':
            try:
                KVDataStoreController.kv_datastore_service.put(body['key'], body['value'])
                self._send_json({"status": HTTPStatus.OK})
            except Exception as e:
                self._send_json({"status": "error", "message": str(e)})

            
        elif self.path == '/batch_put':
            try:
                KVDataStoreController.kv_datastore_service.batch_put(body['batch'])
                self._send_json({"status": "ok"})
            except Exception as e:
                self._send_json({"status": "error", "message": str(e)})
         

    def do_GET(self):
        query = parse_qs(urlparse(self.path).query)
        try:
            if self.path.startswith('/read?'):
                val = KVDataStoreController.kv_datastore_service.read(query.get('key')[0])
                self._send_json({"value": val})
                
            elif self.path.startswith('/range'):
                res = KVDataStoreController.kv_datastore_service.read_range(query.get('start')[0], query.get('end')[0])
                self._send_json(res)
        except Exception as e:
            self._send_json({"status": "error",  "message" : str(e)})


    def do_DELETE(self):
        
        query = parse_qs(urlparse(self.path).query)
        try:
            KVDataStoreController.kv_datastore_service.delete(query.get('key')[0])
            self._send_json({"status": "deleted"})
        except Exception as e:
            self._send_json({"status": "error" , "message" : str(e)})

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir",required=True,help="Data store directory where the WAL and Disk table files are created")
    parser.add_argument("--port", default=8080, help="Port in which the application has to be started")
    parser.add_argument("--mem-threshold", default=1000, help="The maximum number of keys to be kept in memory for the Datastore")
    return parser.parse_args()

if __name__ == "__main__":
    args= parse_args()
    kv_service_obj = KVDataStoreService(args.data_dir, args.mem_threshold)
    KVDataStoreController.kv_datastore_service = kv_service_obj
    server_address = ('', args.port)
    server = HTTPServer(('localhost', args.port), KVDataStoreController)
    logger.info(f"KV Data Store API running on port {args.port}...")
    logger.info(f"KV Data Store API has {args.data_dir} as the data store directory")
    server.serve_forever()
