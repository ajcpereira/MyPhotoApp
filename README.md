# New-Photos

Ferramenta em PySide6 para:

- Fazer scan recursivo de diretórios com fotos/vídeos
- Extrair metadados ricos (EXIF, hashes, vídeo via ffmpeg)
- Indexar tudo em SQLite
- Explorar análises de desenvolvimento (duplicados, live photos, etc.)

## Como usar

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

pip install -r requirements.txt

python main.py
```

Certifica-te que tens `ffmpeg`/`ffprobe` instalados e no PATH.
