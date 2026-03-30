# ==================================================
# EXTRACTOR DE EXPENSAS - PROCESAMIENTO COMPLETO
# ==================================================
# Este notebook procesa todos los PDFs de expensas de la carpeta de Google Drive
# y guarda los datos en Neon PostgreSQL.

# ==================================================
# 1. INSTALACIÓN DE DEPENDENCIAS
# ==================================================
!pip install -q pdfplumber pandas psycopg2-binary PyDrive2

# ==================================================
# 2. IMPORTAR LIBRERÍAS
# ==================================================
import pdfplumber
import pandas as pd
import re
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import os
import io
from google.colab import userdata, auth
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from oauth2client.client import GoogleCredentials

print("✅ Librerías importadas")

# ==================================================
# 3. CONFIGURAR CONEXIÓN A NEON (USANDO SECRETOS)
# ==================================================
try:
    DATABASE_URL = userdata.get('EXPENSAS_ADM_MB')
    print(f"✅ Conexión a Neon configurada ({DATABASE_URL[:30]}...)")
except:
    print("❌ Error: Configura el secreto 'EXPENSAS_ADM_MB' en Colab (ícono 🔑)")
    raise

# ==================================================
# 4. CONECTAR A GOOGLE DRIVE
# ==================================================
print("\n🔐 Conectando a Google Drive...")
auth.authenticate_user()
gauth = GoogleAuth()
gauth.credentials = GoogleCredentials.get_application_default()
drive = GoogleDrive(gauth)
print("✅ Conectado a Google Drive")

# ID de la carpeta (de la URL: https://drive.google.com/drive/folders/19Okcoogy127t38mEPpG13yHcuwvwEAGB)
FOLDER_ID = "19Okcoogy127t38mEPpG13yHcuwvwEAGB"

# ==================================================
# 5. FUNCIONES DE EXTRACCIÓN
# ==================================================

def extract_header(pdf):
    """Extrae datos de cabecera de la página 4"""
    for i, page in enumerate(pdf.pages):
        text = page.extract_text()
        if "SALDO ANTERIOR" in text and "Ingresos de éste mes" in text:
            # Extraer período (puede estar en página 1 o 5)
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

            return {
                'periodo': periodo,
                'saldo_anterior': float(saldo_ant.group(1).replace('.', '').replace(',', '.')) if saldo_ant else 0,
                'ingresos_mes': float(ingresos.group(1).replace('.', '').replace(',', '.')) if ingresos else 0,
                'egresos_mes': float(egresos.group(1).replace('.', '').replace(',', '.')) if egresos else 0,
                'saldo_cierre': float(saldo_cierre.group(1).replace('.', '').replace(',', '.')) if saldo_cierre else 0
            }
    return None

def extract_gastos(pdf):
    """Extrae todos los gastos por rubro"""
    gastos = []
    current_rubro = None

    for page in pdf.pages:
        text = page.extract_text()
        lines = text.split('\n')

        for line in lines:
            # Detectar rubros (formato: "2 SERVICIOS PÚBLICOS")
            rubro_match = re.match(r'^(\d+)\s+([A-ZÁÉÍÓÚÑ\s]+)$', line.strip())
            if rubro_match and len(rubro_match.group(2)) > 3:
                current_rubro = rubro_match.group(2).strip()
                continue

            # Detectar gastos con montos
            if current_rubro and re.search(r'\d+\.\d{3},\d{2}', line):
                monto_match = re.search(r'([\d.,]+)$', line)
                if monto_match:
                    monto_str = monto_match.group(1).replace('.', '').replace(',', '.')
                    proveedor = line[:monto_match.start()].strip()
                    proveedor = re.sub(r'^\d+\.', '', proveedor).strip()

                    # Limpiar proveedor (eliminar números iniciales)
                    proveedor = re.sub(r'^\d+\s+', '', proveedor)

                    gastos.append({
                        'rubro': current_rubro,
                        'proveedor': proveedor[:200],
                        'monto': float(monto_str)
                    })
    return gastos

def extract_prorrateo(pdf):
    """Extrae tabla de prorrateo por unidad funcional"""
    prorrateo = []

    for page in pdf.pages:
        text = page.extract_text()
        lines = text.split('\n')

        for line in lines:
            # Patrón para unidades funcionales
            # Ej: "1 PB-1 DECUZZI JUAN MANUEL 61800,01 61800,01 1854,00 0,6180 74160,00 137.814,01"
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
                        'total_expensas': float(uf_match.group(8).replace('.', '').replace(',', '.')) if len(uf_match.groups()) >= 8 else 0
                    })
                except Exception as e:
                    continue
    return prorrateo

def save_to_neon(liquidacion_data, gastos, prorrateo, nombre_archivo, conn_string):
    """Guarda todos los datos en Neon"""
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    try:
        # Verificar si ya existe este período
        cur.execute("SELECT id FROM liquidaciones WHERE periodo = %s", (liquidacion_data['periodo'],))
        existing = cur.fetchone()

        if existing:
            liquidacion_id = existing[0]
            # Eliminar datos anteriores
            cur.execute("DELETE FROM gastos WHERE liquidacion_id = %s", (liquidacion_id,))
            cur.execute("DELETE FROM prorrateo WHERE liquidacion_id = %s", (liquidacion_id,))
            # Actualizar liquidación
            cur.execute("""
                UPDATE liquidaciones
                SET saldo_anterior = %s, ingresos_mes = %s, egresos_mes = %s, saldo_cierre = %s
                WHERE id = %s
            """, (
                liquidacion_data['saldo_anterior'],
                liquidacion_data['ingresos_mes'],
                liquidacion_data['egresos_mes'],
                liquidacion_data['saldo_cierre'],
                liquidacion_id
            ))
            print(f"   📝 Actualizada liquidación existente ID={liquidacion_id}")
        else:
            # Insertar nueva liquidación
            cur.execute("""
                INSERT INTO liquidaciones
                (periodo, saldo_anterior, ingresos_mes, egresos_mes, saldo_cierre, fecha_emision)
                VALUES (%s, %s, %s, %s, %s, CURRENT_DATE)
                RETURNING id
            """, (
                liquidacion_data['periodo'],
                liquidacion_data['saldo_anterior'],
                liquidacion_data['ingresos_mes'],
                liquidacion_data['egresos_mes'],
                liquidacion_data['saldo_cierre']
            ))
            liquidacion_id = cur.fetchone()[0]
            print(f"   ✨ Nueva liquidación ID={liquidacion_id}")

        # Guardar gastos
        for gasto in gastos:
            cur.execute("""
                INSERT INTO gastos (liquidacion_id, rubro, proveedor, monto)
                VALUES (%s, %s, %s, %s)
            """, (liquidacion_id, gasto['rubro'], gasto['proveedor'], gasto['monto']))

        # Guardar prorrateo
        for p in prorrateo:
            cur.execute("""
                INSERT INTO prorrateo
                (liquidacion_id, uf, propietario, saldo_anterior, pagos, deuda, intereses, total_expensas)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                liquidacion_id, p['uf'], p['propietario'],
                p['saldo_anterior'], p['pagos'], p['deuda'],
                p['intereses'], p['total_expensas']
            ))

        # Registrar archivo procesado
        cur.execute("""
            INSERT INTO archivos_procesados (nombre_archivo, liquidacion_id)
            VALUES (%s, %s)
            ON CONFLICT (nombre_archivo) DO UPDATE SET liquidacion_id = EXCLUDED.liquidacion_id
        """, (nombre_archivo, liquidacion_id))

        conn.commit()
        return liquidacion_id

    except Exception as e:
        conn.rollback()
        print(f"   ❌ Error al guardar: {e}")
        return None
    finally:
        cur.close()
        conn.close()

# ==================================================
# 6. PROCESAR TODOS LOS PDFS DE LA CARPETA
# ==================================================

print("\n" + "="*60)
print("🚀 INICIANDO PROCESAMIENTO DE EXPENSAS")
print("="*60)

# Obtener lista de archivos PDF en la carpeta
file_list = drive.ListFile({'q': f"'{FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false"}).GetList()

print(f"\n📁 Encontrados {len(file_list)} archivos PDF")

# Ordenar por nombre (que incluye fecha)
file_list.sort(key=lambda x: x['title'])

resultados = []

for file in file_list:
    nombre = file['title']
    print(f"\n📄 Procesando: {nombre}")

    # Initialize temp_pdf_path here to ensure it's always defined for cleanup
    temp_pdf_path = f"/tmp/{nombre}"

    try:
        # Download the file directly to the temporary path
        file.GetContentFile(temp_pdf_path)

        with pdfplumber.open(temp_pdf_path) as pdf:
            # Extraer datos
            header = extract_header(pdf)
            if not header or header.get('periodo') is None:
                print(f"   ⚠️ No se pudo extraer cabecera o período, saltando...")
                os.remove(temp_pdf_path)
                resultados.append({
                    'archivo': nombre,
                    'periodo': None,
                    'liquidacion_id': None,
                    'gastos': 0,
                    'unidades': 0,
                    'status': 'ERROR: Cabecera o período no encontrados'
                })
                continue

            gastos = extract_gastos(pdf)
            prorrateo = extract_prorrateo(pdf)

            print(f"   📊 Período: {header['periodo']}")
            print(f"   💰 Gastos: {len(gastos)} registros")
            print(f"   🏢 Unidades: {len(prorrateo)} registros")

            if len(gastos) > 0 and len(prorrateo) > 0:
                # Guardar en Neon
                liquidacion_id = save_to_neon(header, gastos, prorrateo, nombre, DATABASE_URL)

                resultados.append({
                    'archivo': nombre,
                    'periodo': header['periodo'],
                    'liquidacion_id': liquidacion_id,
                    'gastos': len(gastos),
                    'unidades': len(prorrateo),
                    'status': 'OK'
                })
            else:
                print(f"   ⚠️ Datos incompletos, no se guardó")
                resultados.append({
                    'archivo': nombre,
                    'periodo': header['periodo'],
                    'liquidacion_id': None,
                    'gastos': 0,
                    'unidades': 0,
                    'status': 'INCOMPLETO'
                })

        os.remove(temp_pdf_path) # Clean up the temporary file

    except Exception as e:
        print(f"   ❌ Error: {e}")
        # Ensure temporary file is cleaned up even on error
        if os.path.exists(temp_pdf_path):
            os.remove(temp_pdf_path)
        resultados.append({
            'archivo': nombre,
            'periodo': None,
            'liquidacion_id': None,
            'gastos': 0,
            'unidades': 0,
            'status': f'ERROR: {str(e)[:50]}'
        })

# ==================================================
# 7. RESUMEN FINAL
# ==================================================

print("\n" + "="*60)
print("📊 RESUMEN DE PROCESAMIENTO")
print("="*60)

df_resultados = pd.DataFrame(resultados)
print(df_resultados.to_string(index=False))

print(f"\n✅ Procesados: {len([r for r in resultados if r['status'] == 'OK'])} archivos")
print(f"⚠️  Incompletos: {len([r for r in resultados if r['status'] == 'INCOMPLETO'])}")
print(f"❌ Errores: {len([r for r in resultados if r['status'].startswith('ERROR')])}")

# Verificar totales en Neon
print("\n" + "="*60)
print("📈 ESTADO FINAL EN NEON")
print("="*60)

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM liquidaciones")
total_liquidaciones = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM gastos")
total_gastos = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM prorrateo")
total_prorrateo = cur.fetchone()[0]

print(f"📋 Liquidaciones: {total_liquidaciones}")
print(f"💰 Gastos: {total_gastos}")
print(f"🏢 Registros de prorrateo: {total_prorrateo}")

# Mostrar últimos períodos
cur.execute("SELECT periodo, saldo_cierre FROM liquidaciones ORDER BY id DESC LIMIT 5")
print("\n📅 Últimos períodos procesados:")
for periodo, saldo in cur.fetchall():
    print(f"   - {periodo}: saldo cierre ${saldo:,.2f}")

cur.close()
conn.close()

print("\n✨ PROCESO COMPLETADO")
