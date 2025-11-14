import os

# Carpetas que no se mostrarán en el árbol
EXCLUIR = {".git", ".venv", "venv", "__pycache__", "env", "logs", "airflow_home", "plugins"}
MAX_NIVEL = 4  # Profundidad máxima del árbol (puedes ajustarla)

def listar_estructura(ruta, nivel=0, max_nivel=MAX_NIVEL):
    estructura = ""
    if nivel >= max_nivel:
        return estructura

    for elemento in sorted(os.listdir(ruta)):
        if elemento in EXCLUIR:
            continue
        path = os.path.join(ruta, elemento)
        estructura += f"{'│   ' * nivel}├── {elemento}\n"
        if os.path.isdir(path):
            estructura += listar_estructura(path, nivel + 1, max_nivel)
    return estructura

if __name__ == "__main__":
    # 👇 Cambia esta ruta por la carpeta raíz de tu proyecto si quieres
    ruta_base = "."

    estructura = f"# Estructura del proyecto\n\n"
    estructura += f"Ruta base: `{os.path.abspath(ruta_base)}`\n\n"
    estructura += "```\n" + listar_estructura(ruta_base) + "```\n"

    # Guarda el resultado en un archivo Markdown
    with open("estructura.md", "w", encoding="utf-8") as f:
        f.write(estructura)

    print("✅ Archivo 'estructura.md' generado correctamente.")