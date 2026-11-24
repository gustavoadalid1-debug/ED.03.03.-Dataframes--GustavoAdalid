# ED.03.03 - DataFrames en PySpark

Gustavo Adalid Catalán Carmen

Práctica de DataFrames en Spark SQL con PySpark:

- `ejercicios.py`: los 8 ejercicios prácticos (creación, filtrado, columnas, agregaciones, window functions, joins, SQL puro y funciones de string).
- `proyecto_ventas.py`: proyecto de análisis de ventas por país con `pdi_sales.csv`.

## Requisitos (Fedora Linux)

### 1. Paquetes del sistema

```bash
sudo dnf install -y git wget python3 python3-pip java-17-openjdk-headless
```

Comprueba la versión de Java:

```bash
java -version
```

Si tienes varias versiones de Java instaladas, elige la 17 con `sudo alternatives --config java`.

### 2. Clonar el repositorio

```bash
git clone https://github.com/gustavoadalid1-debug/ED.03.03.-Dataframes--GustavoAdalid.git
cd ED.03.03.-Dataframes--GustavoAdalid
```

### 3. Entorno virtual e instalación de PySpark

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Si Spark no encuentra Java, define `JAVA_HOME` antes de ejecutar:

```bash
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
```

### 4. Descargar el dataset

```bash
mkdir -p DATOS
wget "https://raw.githubusercontent.com/josepgarcia/datos/main/pdi_sales.csv" -P ./DATOS
```

El archivo usa `;` como separador y queda en `DATOS/pdi_sales.csv` (no se sube al repositorio).

## Ejecución

Con el entorno virtual activado, desde la raíz del repositorio:

```bash
python ejercicios.py
python proyecto_ventas.py
```

Para guardar las salidas en un archivo:

```bash
python ejercicios.py > salida_ejercicios.txt
python proyecto_ventas.py > salida_proyecto.txt
```

Mientras un script corre se puede abrir la interfaz de Spark en <http://localhost:4040>.

## Notas sobre los datos

Las columnas `Zip` y `Country` traen espacios sobrantes al final en el CSV original (por ejemplo `France `), por eso el proyecto aplica `trim` a ambas antes de filtrar o agrupar.
