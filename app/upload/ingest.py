# app/upload/ingest.py
import io
import zipfile
import shutil
import re
import uuid
import json
from pathlib import Path

SAFE_UPLOAD = Path("/tmp/uploads")
SAFE_UPLOAD.mkdir(parents=True, exist_ok=True)

ENCODINGS = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]

# Palavras-chave para detecção
MYSTERY_KEYWORDS = ["mystery"]
PKO_KEYWORDS = ["bounty", "bounties", "progressive", "pko", "ko", "knockout"]

def smart_read_text(path: Path) -> str:
    data = path.read_bytes()
    for enc in ENCODINGS:
        try:
            return data.decode(enc).replace("\r\n", "\n").replace("\r", "\n")
        except UnicodeDecodeError:
            continue
    return data.decode("latin-1", errors="ignore")

def detect_bucket(filename: str, content: str) -> str:
    """
    Detecta o tipo de torneio baseado no nome do arquivo E conteúdo.
    Prioridade: Mystery > PKO > NON-KO
    """
    # Combinar nome do arquivo e conteúdo para busca (case-insensitive)
    search_text = (filename + " " + content).lower()
    
    # 1. Mystery - se contém "mystery" no nome OU conteúdo
    for keyword in MYSTERY_KEYWORDS:
        if keyword.lower() in search_text:
            return "MYSTERY"
    
    # 2. PKO - se contém qualquer palavra PKO no nome OU conteúdo
    for keyword in PKO_KEYWORDS:
        if keyword.lower() in search_text:
            return "PKO"
    
    # 3. NON-KO - fallback
    return "NON_KO"

def ingest_zip(file_bytes: bytes) -> dict:
    token = uuid.uuid4().hex
    root = SAFE_UPLOAD / token
    raw_dir = root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
        for zi in zf.infolist():
            if zi.is_dir(): 
                continue
            name = Path(zi.filename).name
            if not name.lower().endswith(".txt"): 
                continue
            out = raw_dir / name
            out.write_bytes(zf.read(zi))

    # flatten, normalize and classify
    classified = {"NON_KO": [], "PKO": [], "MYSTERY": []}
    
    # Criar pastas de classificação
    for bucket in classified.keys():
        (root / bucket).mkdir(exist_ok=True)
    
    for path in raw_dir.glob("*.txt"):
        text = smart_read_text(path)
        # Detecta bucket usando nome do arquivo E conteúdo
        bucket = detect_bucket(path.name, text)
        
        # Mover arquivo para pasta classificada
        target_dir = root / bucket
        target_path = target_dir / path.name
        
        # re‑write normalizado (UTF‑8 LF) no destino
        target_path.write_text(text, encoding="utf-8")
        path.unlink()  # Remove do raw/
        
        classified[bucket].append(str(target_path))

    manifest = {
        "token": token,
        "counts": {k: len(v) for k, v in classified.items()},
        "files": classified,
        "root": str(root),
    }
    (root / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    return manifest