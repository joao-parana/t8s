import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 1. Criar dados de exemplo com segmentos ausentes
np.random.seed(42)

# Criar três segmentos de tempo (exemplo)
seg1 = pd.date_range(start='2024-01-01', end='2024-01-01 12:00', freq='5T')
seg2 = pd.date_range(start='2024-01-01 15:00', end='2024-01-01 18:00', freq='5T')
seg3 = pd.date_range(start='2024-01-02 00:00', end='2024-01-02 06:00', freq='5T')

# Combinar os segmentos para simular períodos sem dados
timestamps = seg1.union(seg2).union(seg3)

# Gerar valores aleatórios para os dados presentes
df = pd.DataFrame(index=timestamps, data={'valor': np.random.randn(len(timestamps)) * 10 + 50})

# Criar um índice completo para identificar os períodos ausentes
full_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='5T')
df = df.reindex(full_index)  # Introduz NaNs onde os dados estão ausentes

# 2. Interpolar os valores ausentes (método 'time' considera o intervalo temporal)
df['interpolado'] = df['valor'].interpolate(method='time')

# 3. Identificar lacunas (períodos onde o dado original é NaN)
nan_mask = df['valor'].isna()

# 4. Plotar o gráfico
plt.figure(figsize=(12, 6))

# Dados originais (apenas pontos não NaN)
plt.plot(df.index, df['valor'], label='Dados Reais', color='blue', marker='o', 
         linestyle='-', markersize=3, linewidth=1)

# Interpolações (linha tracejada)
plt.plot(df.index, df['interpolado'], label='Interpolação', color='red', 
         linestyle='--', linewidth=1)

# Destacar áreas ausentes com fundo vermelho semi-transparente
in_gap = False
gap_start = None
for ts in df.index:
    if nan_mask[ts] and not in_gap:
        gap_start = ts
        in_gap = True
    elif not nan_mask[ts] and in_gap:
        gap_end = ts
        plt.axvspan(gap_start, gap_end, color='red', alpha=0.1)
        in_gap = False

# Caso o último período esteja em uma lacuna
if in_gap:
    plt.axvspan(gap_start, df.index[-1], color='red', alpha=0.1)

plt.xlabel('Tempo')
plt.ylabel('Valor do Sensor')
plt.title('Série Temporal com Interpolações e Dados Ausentes Destacados')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

