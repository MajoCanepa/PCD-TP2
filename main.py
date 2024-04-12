import sys
import MySQLdb
import csv 

def main():
    # Leel archivo CSV
    datos_csv = []
    try:
        with open('localidades.csv', newline='') as archivo_csv:
            lector_csv = csv.reader(archivo_csv, delimiter=',', quotechar='"')
            for fila in lector_csv:
                datos_csv.append(fila)
                print(fila)
        print('Datos leídos correctamente')
    except FileNotFoundError:
        print("El archivo 'localidades.csv' no se encuentra en el directorio actual.")
        sys.exit(1)

    # Conectar a la base de datos
    try:
        db = MySQLdb.connect("localhost", "root", "", "testdb")
        cursor = db.cursor()
        print('Conexión correcta a la base de datos')
    except MySQLdb.Error as e:
        print('Error al conectar a la base de datos:', e)
        sys.exit(1)

    # Crear la tabla en la base de datos
    try:
        cursor.execute("DROP TABLE IF EXISTS localidades")  
        sql = """      
            CREATE TABLE IF NOT EXISTS localidades (
                provincia VARCHAR(255),
                id INT,
                localidad VARCHAR(255),
                cp INT,
                id_prov_mstr INT
            )
        """
        cursor.execute(sql)
        db.commit()
        print('Tabla de localidades creada')
    except MySQLdb.Error as e:
        print('Error al crear la tabla:', e)
        db.rollback()
        sys.exit(1)

    # Insertar los datos del CSV a la tabla
    try:
       
        for fila in datos_csv:  
            provincia, id_localidad, localidad, cp, id_prov_mstr = fila
            sql = "INSERT INTO localidades (provincia, id, localidad, cp, id_prov_mstr) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(sql, (provincia, id_localidad, localidad, cp, id_prov_mstr))
        db.commit()
        print('Los datos fueron insertados a la tabla')
    except MySQLdb.Error as e:
        print('Error al insertar datos:', e)
        db.rollback()
        sys.exit(1)

    # Exportar los datos por provincia a archivos CSV separados
    try:
        cursor.execute("SELECT DISTINCT provincia FROM localidades")
        provincias = cursor.fetchall()
        
        for provincia in provincias:
            provincia = provincia[0]
            cursor.execute("SELECT localidad FROM localidades WHERE provincia = %s", (provincia,))
            localidades = [localidad[0] for localidad in cursor.fetchall()]
            localidades = [localidad for localidad in localidades if localidad.strip()] 
            archivo_csv = provincia + '.csv'
            with open(archivo_csv, 'w', newline='') as archivo:
                escritor_csv = csv.writer(archivo)
                for localidad in localidades:
                    escritor_csv.writerow([localidad])
                escritor_csv.writerow(['Cantidad total de localidades:', len(localidades)])
            print(f"Archivo {archivo_csv} creado")
        print('Exportación completada')
    except MySQLdb.Error as e:
        print('Error al exportar datos:', e)
        sys.exit(1)
    finally:
        db.close()
main()
