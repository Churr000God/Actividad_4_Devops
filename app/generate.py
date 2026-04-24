import csv
import json
import html as html_lib
from pathlib import Path
from datetime import datetime, UTC

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = BASE_DIR / "data" / "Estados.txt"
DIST_DIR = BASE_DIR / "dist"
LOGS_DIR = BASE_DIR / "logs"
TEMPLATE_FILE = BASE_DIR / "app" / "templates" / "index.tpl.html"
JSON_FILE = DIST_DIR / "estados.json"
DDB_JSON_FILE = DIST_DIR / "estados_dynamodb.json"
HTML_FILE = DIST_DIR / "index.html"
APP_LOG = LOGS_DIR / "build.log"


def ensure_dirs():
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


def log(message: str):
    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    with APP_LOG.open("a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)


def read_txt_as_csv(file_path: Path):
    rows = []
    with file_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cleaned = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
            rows.append(cleaned)
    return rows


def save_json(data, output_path: Path):
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def build_table_rows(rows):
    html_rows = []
    for row in rows:
        html_rows.append(
            f"""
        <tr>
          <td>{row.get('Estado', '')}</td>
          <td>{row.get('Temperatura', '')} °C</td>
          <td>{row.get('Humedad', '')} %</td>
          <td>${row.get('Costo_Alojamiento', '')}</td>
          <td>${row.get('Costo_Transporte', '')}</td>
          <td>{row.get('Dias_Promedio', '')}</td>
          <td>{row.get('Tiempo_Traslado', '')} h</td>
        </tr>
        """
        )
    return "\n".join(html_rows)


def normalize_number_string(value: str) -> str:
    return value.strip().replace(",", "")


def to_dynamodb_attribute_value(value, force_type: str | None = None):
    if value is None:
        return {"NULL": True}

    value_str = str(value).strip()
    if value_str == "":
        return {"NULL": True}

    if force_type == "S":
        return {"S": value_str}
    if force_type == "N":
        return {"N": normalize_number_string(value_str)}

    number_candidate = normalize_number_string(value_str)
    try:
        float(number_candidate)
        return {"N": number_candidate}
    except ValueError:
        return {"S": value_str}


def build_dynamodb_items(rows):
    numeric_columns = {
        "Temperatura",
        "Humedad",
        "Costo_Alojamiento",
        "Costo_Transporte",
        "Dias_Promedio",
        "Tiempo_Traslado",
    }

    items = []
    for row in rows:
        item = {}
        for key, value in row.items():
            if key == "Estado":
                item[key] = to_dynamodb_attribute_value(value, force_type="S")
            elif key in numeric_columns:
                item[key] = to_dynamodb_attribute_value(value, force_type="N")
            else:
                item[key] = to_dynamodb_attribute_value(value)
        items.append(item)
    return items


def generate_html(rows, template_path: Path, output_path: Path, ddb_json_pretty: str):
    with template_path.open("r", encoding="utf-8") as f:
        template = f.read()

    rendered_html = template.format(
        total_registros=len(rows),
        fecha_generacion=datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
        table_rows=build_table_rows(rows),
        json_filename="estados.json",
        ddb_json_filename="estados_dynamodb.json",
        ddb_json_pretty=html_lib.escape(ddb_json_pretty),
    )

    with output_path.open("w", encoding="utf-8") as f:
        f.write(rendered_html)


def main():
    ensure_dirs()
    log("Inicio de ejecución.")

    if not DATA_FILE.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {DATA_FILE}")

    rows = read_txt_as_csv(DATA_FILE)
    log(f"Archivo leído correctamente: {DATA_FILE}")
    log(f"Registros detectados: {len(rows)}")

    save_json(rows, JSON_FILE)
    log(f"JSON generado en: {JSON_FILE}")

    ddb_items = build_dynamodb_items(rows)
    save_json(ddb_items, DDB_JSON_FILE)
    log(f"JSON DynamoDB generado en: {DDB_JSON_FILE}")

    ddb_json_pretty = json.dumps(ddb_items, ensure_ascii=False, indent=2)
    generate_html(rows, TEMPLATE_FILE, HTML_FILE, ddb_json_pretty=ddb_json_pretty)
    log(f"HTML generado en: {HTML_FILE}")

    generated_at_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    manifest = {
        "generated_at_utc": generated_at_utc,
        "source_file": DATA_FILE.name,
        "total_records": len(rows),
        "outputs": ["index.html", "estados.json", "estados_dynamodb.json"],
    }
    save_json(manifest, DIST_DIR / "manifest.json")
    log("Manifest generado.")

    log("Proceso completado con éxito.")


if __name__ == "__main__":
    main()
