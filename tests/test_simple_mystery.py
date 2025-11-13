"""Teste simples para confirmar detecção de mystery"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.upload.ingest import detect_bucket

print("=" * 60)
print("TESTE DE DETECÇÃO SIMPLES")
print("=" * 60)

print("\n✅ Regra MYSTERY: apenas a palavra 'mystery'")
print("-" * 40)

tests = [
    # Com mystery -> sempre MYSTERY
    ("file.txt", "mystery", "MYSTERY"),
    ("file.txt", "Mystery Tournament", "MYSTERY"), 
    ("file.txt", "MYSTERY", "MYSTERY"),
    ("mystery.txt", "content", "MYSTERY"),
    ("file.txt", "text with mystery inside", "MYSTERY"),
    
    # Com mystery E bounty -> ainda é MYSTERY (mystery tem prioridade)
    ("file.txt", "mystery bounty", "MYSTERY"),
    ("mystery_bounty.txt", "content", "MYSTERY"),
    ("file.txt", "mystery knockout progressive bounty", "MYSTERY"),
    
    # Sem mystery mas com bounty -> PKO
    ("file.txt", "bounty", "PKO"),
    ("bounty.txt", "content", "PKO"),
    ("file.txt", "bounty hunters", "PKO"),
    
    # Sem mystery e sem palavras PKO -> NON-KO
    ("file.txt", "regular tournament", "NON_KO"),
]

for filename, content, expected in tests:
    result = detect_bucket(filename, content)
    status = "✓" if result == expected else "✗"
    
    # Destaque especial para casos com mystery E bounty
    special = ""
    content_lower = content.lower()
    if "mystery" in content_lower and any(word in content_lower for word in ["bounty", "ko", "progressive"]):
        special = " 👈 (tem mystery + outras palavras PKO)"
    
    print(f"{status} '{content}' → {result}{special}")
    assert result == expected

print("\n" + "=" * 60)
print("✅ CONFIRMADO!")
print("=" * 60)
print("\nREGRAS SIMPLES:")
print("1. Se tem 'mystery' → MYSTERY")
print("2. Se não tem 'mystery' mas tem palavras PKO → PKO")  
print("3. Se não tem nada → NON-KO")
print("\n⚠️ IMPORTANTE: Um arquivo com 'mystery bounty' é MYSTERY")
print("   porque tem a palavra 'mystery', não importa o que mais tenha!")