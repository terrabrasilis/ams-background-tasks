import re


def normalize_text(text: str) -> str:
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


_DS = """
Esse indicador inclui as classes do DETER Desmatamento Corte Raso e Desmatamento com
Vegetação. O Desmatamento corte raso corresponde a áreas em que se observa a
supressão completa da vegetação nativa, independentemente de qualquer evidência
de uso posterior. O Desmatamento com vegetação equivale a áreas em que há
evidência de supressão da vegetação nativa e, ao mesmo tempo, de outra cobertura
vegetal."""

_DG = """
Esse indicador inclui as classes do DETER Degradação e Cicatriz de Incêndio Florestal.
A Degradação corresponde a áreas onde houve perda incompleta do dossel florestal, com 
consequente exposição do solo, persistindo uma vegetação com indivíduos arbóreos e/ou com 
estrutura semelhante a estágios inicial e intermediário inicial de sucessão florestal. 
A Cicatriz de Incêndio Florestal corresponde a áreas onde se identificam sinais de alterações 
na cobertura vegetal em decorrência da ação do fogo, podendo ou não persistir vegetação 
arbórea.
"""

_CS = """
Esse indicador inclui as classes do DETER Corte Seletivo Desordenado e Corte Seletivo Geométrico. O 
Corte Seletivo Desordenado corresponde a áreas associadas à exploração madeireira convencional,
em que os indivíduos arbóreos de espécies de interesse comercial são removidos sem
planejamento. O Corte Seletivo Geométrico corresponde a áreas associadas à exploração 
madeireira com plano de manejo, caracterizadas pela disposição regular e geométrica das 
estradas e pátios de estocagem no interior da floresta.
"""

_MN = """
Esse indicador inclui a classe Mineração do DETER, a qual corresponde a áreas de vegetação nativa que foram
convertidas em áreas de atividade de extração mineral. Predominam, nessa classe, as atividades
de garimpo artesanal.
"""

_FOCOS = """
Esse indicador apresenta registros de ocorrência de fogo ativo detectados por sensores orbitais no âmbito do Programa Queimadas (INPE).
"""

_RISCO_DE_DESMATAMENTO = """
Esse indicador apresenta dados de um Modelo de Inteligência Artificial (IA) capaz de prever,
com até 15 dias de antecedência, áreas com maior risco de desmatamento na Amazônia Legal.
O indicador varia de 0 a 1, representando do menor ao maior risco.
"""

_RISCO_DE_ESPALHAMENTO_DO_FOGO = """
Esse indicador do Projeto FIP Cerrado apresenta dados de um modelo de simulação de espalhamento do fogo, baseado em
pontos de ignição mapeados por satélite pelo Programa Queimadas (INPE), no combustível (biomassa seca) e
na umidade da vegetação, derivados de imagens de satélite MODIS/Terra.
"""

_FOCOS_DE_HOJE = """
Esse indicador apresenta registros de ocorrência de fogo ativo detectados por sensores
orbitais, correspondentes ao dia corrente, no âmbito do Programa Queimadas (INPE).
"""

_AI = """
Esse indicador apresenta os dados do incremento anual de desmatamento mapeado pelo PRODES.
O incremento anual corresponde à área de vegetação nativa suprimida identificada em cada ano de monitoramento,
permitindo acompanhar a dinâmica temporal do desmatamento e comparar sua evolução entre diferentes períodos.
"""

_AD = """
Esse indicador apresenta os dados de desmatamento acumulado mapeados pelo PRODES. O desmatamento acumulado
corresponde à soma das áreas de vegetação nativa suprimidas ao longo dos anos de monitoramento.
"""

_DR = """
Esse indicador apresenta a razão entre a área de desmatamento acumulado e a área de vegetação natural disponível.
O índice permite comparar a extensão já desmatada com a cobertura vegetal remanescente na área analisada. Valores iguais a 0
indicam ausência de desmatamento acumulado. Valores menores que 1 indicam que a área de vegetação natural disponível é maior que a área desmatada.
Um valor igual a 1 indica que as áreas desmatada e de vegetação natural disponível possuem a mesma extensão.
Valores superiores a 1 indicam que a área desmatada é maior que a área de vegetação natural remanescente.
Quanto maior o valor do indicador, maior o grau de comprometimento da cobertura vegetal.
"""


def get_description_from_classname(classname: str):
    descriptions = {
        "DS": normalize_text(_DS),
        "DG": normalize_text(_DG),
        "CS": normalize_text(_CS),
        "MN": normalize_text(_MN),
        "AF": normalize_text(_FOCOS),
        "RK": "",
        "RI": normalize_text(_RISCO_DE_DESMATAMENTO),
        "FS": normalize_text(_RISCO_DE_ESPALHAMENTO_DO_FOGO),
        "FT": normalize_text(_FOCOS_DE_HOJE),
        "AI": normalize_text(_AI),
        "AD": normalize_text(_AD),
        "DR": normalize_text(_DR),
        "NV": "",
    }

    assert classname in descriptions

    return descriptions[classname]
