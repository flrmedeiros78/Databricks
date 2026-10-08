"""
Teste de validação do InvestFacil Web - index.html
Data: 2026-10-08

Testes realizados:
1. Correção dos cards roxos (DY não mais multiplicado por 100)
2. Botão "Ir para o site" ao lado do listbox fonte-info
3. Autocomplete na busca de ticker (busca por ticker, nome_curto, nome_completo)
4. Dividend Yield Mínimo com step=0.01 e lang=pt-BR
5. P/L Máximo com step=1 (inteiro)
6. Exportação Excel: integrity hash removido do CDN SheetJS
7. Botão "Buscar" para aplicar filtros manualmente
8. Botão "Limpar Filtros" para resetar
"""

import json
import re
import os

INDEX_HTML_PATH = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'InvestFacilWeb', 'index.html')
INDICADORES_JSON_PATH = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'InvestFacilWeb', 'indicadores.json')


def load_html():
    with open(INDEX_HTML_PATH, 'r', encoding='utf-8') as f:
        return f.read()


def load_data():
    with open(INDICADORES_JSON_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def test_dy_not_multiplied_by_100():
    """Teste 1: DY não deve ser multiplicado por 100 nos cards roxos"""
    html = load_html()
    data = load_data()

    # Verificar que não existe mais '* 100' no cálculo de DY
    assert '* 100).toFixed(2)' not in html, "DY ainda está sendo multiplicado por 100!"
    assert '(acao.dividend_yield * 100)' not in html, "DY ainda multiplicado por 100 na exibição!"

    # Calcular média real
    acoes_com_dy = [a for a in data if a.get('dividend_yield') and a['dividend_yield'] > 0]
    media_dy = sum(a['dividend_yield'] for a in acoes_com_dy) / len(acoes_com_dy)

    # A média deve ser um valor razoável (entre 0 e 100%)
    assert 0 < media_dy < 100, f"Média DY fora do range esperado: {media_dy}"

    print(f"✅ Teste 1 PASSOU: Média DY = {media_dy:.2f}% (sem multiplicação por 100)")


def test_fonte_info_button():
    """Teste 2: Botão 'Ir para o site' ao lado do listbox"""
    html = load_html()
    assert 'btn-ir-site' in html, "Botão 'Ir para o site' não encontrado!"
    assert 'abrirSiteFonte' in html, "Função abrirSiteFonte() não encontrada!"
    assert 'window.open' in html, "Função não abre nova aba!"
    print("✅ Teste 2 PASSOU: Botão 'Ir para o site' presente")


def test_autocomplete():
    """Teste 3: Autocomplete na busca de ticker"""
    html = load_html()
    data = load_data()

    assert 'ticker-suggestions' in html, "Container de sugestões não encontrado!"
    assert 'mostrarSugestoes' in html, "Função mostrarSugestoes() não encontrada!"
    assert 'selecionarTicker' in html, "Função selecionarTicker() não encontrada!"

    # Testar busca por 'petro'
    termo = 'petro'
    matches = [a for a in data if termo in (a.get('ticker') or '').lower()
               or termo in (a.get('nome_curto') or '').lower()
               or termo in (a.get('nome_completo') or '').lower()]
    assert len(matches) > 0, f"Nenhum match para '{termo}'!"
    assert 'PETR4' in [m['ticker'] for m in matches], "PETR4 não encontrado na busca por 'petro'!"

    # Testar busca por 'banco'
    termo = 'banco'
    matches = [a for a in data if termo in (a.get('ticker') or '').lower()
               or termo in (a.get('nome_curto') or '').lower()
               or termo in (a.get('nome_completo') or '').lower()]
    assert len(matches) >= 2, f"Esperado pelo menos 2 matches para 'banco'"

    print(f"✅ Teste 3 PASSOU: Autocomplete funciona para 'petro', 'banco', etc.")


def test_dy_input_decimal():
    """Teste 4: Input DY com step=0.01 e lang=pt-BR"""
    html = load_html()
    assert 'step="0.01"' in html, "step=0.01 não encontrado no input DY!"
    assert 'lang="pt-BR"' in html, "lang=pt-BR não encontrado!"
    print("✅ Teste 4 PASSOU: DY input com step=0.01 e lang=pt-BR")


def test_pl_input_integer():
    """Teste 5: Input P/L com step=1 (inteiro)"""
    html = load_html()
    # Procurar o input filter-pl específico
    pl_match = re.search(r'id="filter-pl"[^>]*step="(\d+)"', html)
    assert pl_match, "Input filter-pl não encontrado!"
    assert pl_match.group(1) == '1', f"step={pl_match.group(1)} mas esperado 1!"
    print("✅ Teste 5 PASSOU: P/L input com step=1 (inteiro)")


def test_excel_cdn_no_integrity():
    """Teste 6: CDN SheetJS sem integrity hash"""
    html = load_html()
    assert 'integrity="sha512' not in html, "Integrity hash ainda presente no CDN!"
    assert 'xlsx.full.min.js' in html, "SheetJS CDN não encontrado!"
    print("✅ Teste 6 PASSOU: SheetJS CDN sem integrity hash bloqueante")


def test_search_button():
    """Teste 7: Botão 'Buscar' para aplicar filtros manualmente"""
    html = load_html()
    assert 'btn-buscar' in html, "Botão 'Buscar' não encontrado!"
    assert 'btn-limpar' in html, "Botão 'Limpar Filtros' não encontrado!"
    assert 'limparFiltros' in html, "Função limparFiltros() não encontrada!"

    # Auto-apply deve ser removido dos filtros
    assert "filter-setor').addEventListener('change', aplicarFiltros)" not in html, "Auto-apply ainda no setor!"
    assert "filter-dy').addEventListener('input', aplicarFiltros)" not in html, "Auto-apply ainda no DY!"
    assert "filter-pl').addEventListener('input', aplicarFiltros)" not in html, "Auto-apply ainda no P/L!"

    # Ticker deve mostrar sugestões, não aplicar filtros
    assert "filter-ticker').addEventListener('input', mostrarSugestoes)" in html, "Ticker não está mostrando sugestões!"

    print("✅ Teste 7 PASSOU: Botão Buscar e Limpar funcionando, auto-apply removido")


def test_html_structure():
    """Teste 8: Estrutura HTML balanceada"""
    html = load_html()
    for tag in ['html', 'head', 'body', 'div', 'script', 'style', 'table', 'select', 'button']:
        opens = len(re.findall(f'<{tag}[ >]', html))
        closes = len(re.findall(f'</{tag}>', html))
        assert opens == closes, f"Tags <{tag}> desbalanceadas: {opens} abertas vs {closes} fechadas"
    print("✅ Teste 8 PASSOU: Estrutura HTML balanceada")


def test_all_functions_present():
    """Teste 9: Todas as funções JS presentes"""
    html = load_html()
    functions = [
        'carregarDados', 'atualizarEstatisticas', 'popularSetores',
        'aplicarFiltros', 'exibirResultados', 'formatarNumero',
        'getTickerInfoUrl', 'carregarSheetJS', 'exportarExcel',
        'exportarCSV', 'exportarJSON', 'abrirSiteFonte',
        'mostrarSugestoes', 'selecionarTicker', 'limparFiltros'
    ]
    for func in functions:
        assert f'function {func}' in html, f"Função {func}() não encontrada!"
    print(f"✅ Teste 9 PASSOU: Todas as {len(functions)} funções JS presentes")


def run_all_tests():
    """Executa todos os testes"""
    print("=" * 60)
    print("  InvestFacil Web - Testes de Validação")
    print("  Data: 2026-10-08")
    print("=" * 60)
    print()

    tests = [
        test_dy_not_multiplied_by_100,
        test_fonte_info_button,
        test_autocomplete,
        test_dy_input_decimal,
        test_pl_input_integer,
        test_excel_cdn_no_integrity,
        test_search_button,
        test_html_structure,
        test_all_functions_present,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"❌ {test.__name__} FALHOU: {e}")
            failed += 1
        except Exception as e:
            print(f"❌ {test.__name__} ERRO: {e}")
            failed += 1

    print()
    print("=" * 60)
    print(f"  Resultado: {passed} passaram, {failed} falharam")
    print("=" * 60)

    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    exit(0 if success else 1)
