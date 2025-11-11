"""Teste para confirmar as regras de detecção especificadas"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.upload.ingest import detect_bucket

def test_mystery_detection():
    """Mystery: detecta 'mystery' no título OU conteúdo"""
    print("\n🎯 MYSTERY - palavra 'mystery' no título OU conteúdo:")
    
    tests = [
        # No título
        ("mystery_tournament.txt", "regular content", "MYSTERY"),
        ("file_with_mystery.txt", "nothing special", "MYSTERY"),
        ("MYSTERY_GAME.txt", "empty", "MYSTERY"),
        
        # No conteúdo
        ("regular.txt", "This is a mystery tournament", "MYSTERY"),
        ("normal.txt", "MYSTERY BOUNTY special", "MYSTERY"),
        ("file.txt", "Something with Mystery inside", "MYSTERY"),
        
        # Não tem mystery
        ("regular.txt", "normal tournament", "NON_KO"),
        ("bounty.txt", "bounty content", "PKO"),  # PKO tem prioridade
    ]
    
    for filename, content, expected in tests:
        result = detect_bucket(filename, content)
        status = "✓" if result == expected else "✗"
        print(f"  {status} Arquivo: '{filename}' | Conteúdo: '{content[:30]}...' → {result}")
        assert result == expected, f"Erro: esperado {expected}, obteve {result}"


def test_pko_detection():
    """PKO: detecta palavras-chave no título OU conteúdo"""
    print("\n💰 PKO - palavras-chave no título OU conteúdo:")
    print("   Palavras: Bounty, bounties, Progressive, PKO, ko, knockout")
    
    tests = [
        # No título
        ("bounty_hunter.txt", "regular content", "PKO"),
        ("Bounties_special.txt", "nothing", "PKO"),
        ("progressive_tournament.txt", "empty", "PKO"),
        ("PKO_daily.txt", "test", "PKO"),
        ("ko_championship.txt", "content", "PKO"),
        ("knockout_series.txt", "text", "PKO"),
        
        # No conteúdo
        ("regular.txt", "This is a Bounty tournament", "PKO"),
        ("file.txt", "Multiple bounties available", "PKO"),
        ("normal.txt", "Progressive knockout", "PKO"),
        ("test.txt", "PKO special event", "PKO"),
        ("game.txt", "KO tournament here", "PKO"),
        ("play.txt", "Knockout championship", "PKO"),
        
        # Case insensitive
        ("BOUNTY.txt", "content", "PKO"),
        ("file.txt", "PROGRESSIVE", "PKO"),
        ("pko.txt", "test", "PKO"),
        ("file.txt", "KnOcKoUt", "PKO"),
    ]
    
    for filename, content, expected in tests:
        result = detect_bucket(filename, content)
        status = "✓" if result == expected else "✗"
        print(f"  {status} Arquivo: '{filename}' | Conteúdo: '{content[:30]}...' → {result}")
        assert result == expected, f"Erro: esperado {expected}, obteve {result}"


def test_non_ko_fallback():
    """NON-KO: quando não detecta mystery nem PKO"""
    print("\n🎮 NON-KO - fallback quando não detecta nada:")
    
    tests = [
        ("regular.txt", "normal tournament", "NON_KO"),
        ("sunday_million.txt", "big event", "NON_KO"),
        ("main_event.txt", "championship series", "NON_KO"),
        ("turbo.txt", "fast tournament", "NON_KO"),
        ("", "", "NON_KO"),
        ("file.txt", "random text here", "NON_KO"),
    ]
    
    for filename, content, expected in tests:
        result = detect_bucket(filename, content)
        status = "✓" if result == expected else "✗"
        print(f"  {status} Arquivo: '{filename}' | Conteúdo: '{content[:30]}...' → {result}")
        assert result == expected, f"Erro: esperado {expected}, obteve {result}"


def test_priority():
    """Testa prioridade: Mystery > PKO > NON-KO"""
    print("\n⚡ PRIORIDADE - Mystery > PKO > NON-KO:")
    
    tests = [
        # Mystery tem prioridade sobre PKO
        ("mystery_bounty.txt", "content", "MYSTERY"),
        ("file.txt", "mystery knockout tournament", "MYSTERY"),
        ("mystery_pko.txt", "progressive", "MYSTERY"),
        
        # PKO quando não tem mystery
        ("bounty.txt", "no special word", "PKO"),
        ("normal.txt", "knockout event", "PKO"),
    ]
    
    for filename, content, expected in tests:
        result = detect_bucket(filename, content)
        status = "✓" if result == expected else "✗"
        print(f"  {status} Arquivo: '{filename}' | Conteúdo: '{content[:30]}...' → {result}")
        assert result == expected, f"Erro: esperado {expected}, obteve {result}"


if __name__ == "__main__":
    print("=" * 70)
    print("CONFIRMAÇÃO DAS REGRAS DE DETECÇÃO")
    print("=" * 70)
    
    test_mystery_detection()
    test_pko_detection()
    test_non_ko_fallback()
    test_priority()
    
    print("\n" + "=" * 70)
    print("✅ CONFIRMADO! Todas as regras estão funcionando corretamente:")
    print("=" * 70)
    print("\n📋 REGRAS IMPLEMENTADAS:")
    print("\n1️⃣ MYSTERY:")
    print("   • Detecta a palavra 'mystery' (case-insensitive)")
    print("   • ✓ No título do arquivo")
    print("   • ✓ No conteúdo do arquivo")
    print("\n2️⃣ PKO:")
    print("   • Detecta qualquer uma das palavras:")
    print("     - Bounty")
    print("     - bounties")
    print("     - Progressive")
    print("     - PKO")
    print("     - ko")
    print("     - knockout")
    print("   • ✓ No título do arquivo")
    print("   • ✓ No conteúdo do arquivo")
    print("   • ✓ Case-insensitive")
    print("\n3️⃣ NON-KO:")
    print("   • ✓ Fallback quando não detecta mystery nem PKO")
    print("\n🎯 PRIORIDADE: Mystery > PKO > NON-KO")