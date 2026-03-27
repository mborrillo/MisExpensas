#!/usr/bin/env python3
"""
Extractor de expensas para GitHub Actions
Procesa PDFs de Google Drive y guarda en Neon PostgreSQL
"""

import os
import sys
import io
import re
import pdfplumber
import psycopg2
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from datetime import datetime

# Configuración
FOLDER_ID = os.environ.get('19Okcoogy127t38mEPpG13yHcuwvwEAGB')
DATABASE_URL = os.environ.get('EXPENSAS_ADM_MB_CABA_I743')
CREDENTIALS_FILE = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

def get_drive_service():
    """Conecta a Google Drive usando credenciales de service account"""
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    return build('drive', 'v3', credentials=credentials)

def get_pdf_files(service):
    """Obtiene lista de PDFs no procesados"""
    query = f"'{FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])

def download_pdf(service, file_id, file_name):
    """Descarga un PDF como bytes"""
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh

def extract_header(pdf):
    """Extrae datos de cabecera"""
    for page in pdf.pages:
        text = page.extract_text()
        if "SALDO ANTERIOR" in text and "Ingresos de éste mes" in text:
            # Extraer período
            periodo = None
            for p in pdf.pages:
                text_p = p.extract_text()
                periodo_match = re.search(r'Perí[io]do:\s*([A-Z]+/\d{4})', text_p, re.IGNORECASE)
                if periodo_match:
                    periodo = periodo_match.group(1)
                    break
            
            saldo_ant = re.search(r'SALDO ANTERIOR\$\s*([\d.,]+)', text)
            ingresos = re.search(r'Ingresos de éste mes\$\s*([\d.,]+)', text)
            egresos = re.search(r'Egresos realizados en este período\$\s*([\d.,]+)', text)
            saldo_cierre = re.search(r'SALDO AL CIERRE\$\s*([\d.,]+)', text)
            
            if saldo_ant and ingresos and egresos and saldo_cierre:
                return {
                    'periodo': periodo,
                    'saldo_anterior': float(saldo_ant.group(1).replace('.', '').replace(',', '.')),
                    'ingresos_mes': float(ingresos.group(1).replace('.', '').replace(',', '.')),
                    'egresos_mes': float(egresos.group(1).replace('.', '').replace(',', '.')),
                    'saldo_cierre': float(saldo_cierre.group(1).replace('.', '').replace(',', '.'))
                }
    return None

def extract_gastos(pdf):
    """Extrae todos los gastos"""
    gastos = []
    current_rubro = None
    
    for page in pdf.pages:
        text = page.extract_text()
        lines = text.split('\n')
        
        for line in lines:
            rubro_match = re.match(r'^(\d+)\s+([A-ZÁÉÍÓÚÑ\s]+)$', line.strip())
            if rubro_match and len(rubro_match.group(2)) > 3:
                current_rubro = rubro_match.group(2).strip()
                continue
            
            if current_rubro and re.search(r'\d+\.\d{3},\d{2}', line):
                monto_match = re.search(r'([\d.,]+)$', line)
                if monto_match:
                    monto_str = monto_match.group(1).replace('.', '').replace(',', '.')
                    proveedor = line[:monto_match.start()].strip()
                    proveedor = re.sub(r'^\d+\.', '', proveedor)
                    proveedor = re.sub(r'^\d+\s+', '', proveedor)
                    
                    gastos.append({
                        'rubro': current_rubro,
                        'proveedor': proveedor[:200],
                        'monto': float(monto_str)
                    })
    return gastos

def extract_prorrateo(pdf):
    """Extrae prorrateo por unidad"""
    prorrateo = []
    
    for page in pdf.pages:
        text = page.extract_text()
        lines = text.split('\n')
        
        for line in lines:
            uf_match = re.match(r'^\s*\d+\s+([A-Z0-9-]+)\s+([A-Z\s]+)\s+([\d.,]+)\s+([-\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)', line)
            if uf_match:
                try:
                    prorrateo.append({
                        'uf': uf_match.group(1),
                        'propietario': uf_match.group(2).strip()[:100],
                        'saldo_anterior': float(uf_match.group(3).replace('.', '').replace(',', '.')),
                        'pagos': float(uf_match.group(4).replace('.', '').replace(',', '.')),
                        'deuda': float(uf_match.group(5).replace('.', '').replace(',', '.')),
                        'intereses': float(uf_match.group(6).replace('.', '').replace(',', '.')),
                        'total_expensas': float(uf_match.group(8).replace('.', '').replace(',', '.'))
                    })
                except:
                    continue
    return prorrateo

def save_to_neon(liquidacion_data, gastos, prorrateo, nombre_archivo):
    """Guarda en Neon"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    try:
        # Verificar si ya existe
        cur.execute("SELECT id FROM liquidaciones WHERE periodo = %s", (liquidacion_data['periodo'],))
        existing = cur.fetchone()
        
        if existing:
            liquidacion_id = existing[0]
            cur.execute("DELETE FROM gastos WHERE liquidacion_id = %s", (liquidacion_id,))
            cur.execute("DELETE FROM prorrateo WHERE liquidacion_id = %s", (liquidacion_id,))
            cur.execute("""
                UPDATE liquidaciones 
                SET saldo_anterior=%s, ingresos_mes=%s, egresos_mes=%s, saldo_cierre=%s
                WHERE id=%s
            """, (liquidacion_data['saldo_anterior'], liquidacion_data['ingresos_mes'],
                  liquidacion_data['egresos_mes'], liquidacion_data['saldo_cierre'], liquidacion_id))
            print(f"   📝 Actualizada: {liquidacion_data['periodo']}")
        else:
            cur.execute("""
                INSERT INTO liquidaciones (periodo, saldo_anterior, ingresos_mes, egresos_mes, saldo_cierre)
                VALUES (%s, %s, %s, %s, %s) RETURNING id
            """, (liquidacion_data['periodo'], liquidacion_data['saldo_anterior'],
                  liquidacion_data['ingresos_mes'], liquidacion_data['egresos_mes'],
                  liquidacion_data['saldo_cierre']))
            liquidacion_id = cur.fetchone()[0]
            print(f"   ✨ Nueva: {liquidacion_data['periodo']}")
        
        # Insertar datos
        for gasto in gastos:
            cur.execute("INSERT INTO gastos (liquidacion_id, rubro, proveedor, monto) VALUES (%s, %s, %s, %s)",
                       (liquidacion_id, gasto['rubro'], gasto['proveedor'], gasto['monto']))
        
        for p in prorrateo:
            cur.execute("""
                INSERT INTO prorrateo (liquidacion_id, uf, propietario, saldo_anterior, pagos, deuda, intereses, total_expensas)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (liquidacion_id, p['uf'], p['propietario'], p['saldo_anterior'],
                  p['pagos'], p['deuda'], p['intereses'], p['total_expensas']))
        
        cur.execute("""
            INSERT INTO archivos_procesados (nombre_archivo, liquidacion_id)
            VALUES (%s, %s) ON CONFLICT (nombre_archivo) DO NOTHING
        """, (nombre_archivo, liquidacion_id))
        
        conn.commit()
        return liquidacion_id
    except Exception as e:
        conn.rollback()
        print(f"   ❌ Error: {e}")
        return None
    finally:
        cur.close()
        conn.close()

def main():
    print("🚀 Iniciando extracción de expensas...")
    print(f"📁 Folder ID: {FOLDER_ID}")
    
    if not FOLDER_ID or not DATABASE_URL:
        print("❌ Faltan variables de entorno")
        sys.exit(1)
    
    # Conectar a Drive
    service = get_drive_service()
    
    # Obtener PDFs
    files = get_pdf_files(service)
    print(f"📄 PDFs encontrados: {len(files)}")
    
    # Procesar cada uno
    for file in files:
        print(f"\n📄 Procesando: {file['name']}")
        
        # Verificar si ya se procesó
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM archivos_procesados WHERE nombre_archivo = %s", (file['name'],))
        if cur.fetchone():
            print("   ⏭️ Ya procesado, saltando...")
            cur.close()
            conn.close()
            continue
        cur.close()
        conn.close()
        
        try:
            # Descargar PDF
            pdf_bytes = download_pdf(service, file['id'], file['name'])
            
            with pdfplumber.open(pdf_bytes) as pdf:
                header = extract_header(pdf)
                if not header:
                    print("   ⚠️ No se pudo extraer cabecera")
                    continue
                
                gastos = extract_gastos(pdf)
                prorrateo = extract_prorrateo(pdf)
                
                print(f"   📊 Período: {header['periodo']}")
                print(f"   💰 Gastos: {len(gastos)}")
                print(f"   🏢 Unidades: {len(prorrateo)}")
                
                if gastos and prorrateo:
                    save_to_neon(header, gastos, prorrateo, file['name'])
                else:
                    print("   ⚠️ Datos incompletos")
                    
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    print("\n✨ Proceso completado")

if __name__ == "__main__":
    main()
