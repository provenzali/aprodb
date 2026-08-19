# Risultati locali — 19 agosto 2026

Comando: `cargo run --release -p aprodb-compute --features gpu --example
compute_crossover`. Hardware: Intel Core i5-1340P e Intel Iris Xe; Windows 11
10.0.26200; Rust 1.97.1. Nove campioni per percorso caldo. Tutti i ranking
top-20 GPU hanno coinciso con la CPU entro tolleranza relativa `1e-4`.

| Righe × width | Batch byte | CPU p50/p95/p99 µs | GPU fredda µs | GPU calda p50/p95/p99 µs | CPU righe/s | GPU righe/s |
|---:|---:|---:|---:|---:|---:|---:|
| 1.024 × 64 | 262.272 | 824 / 1.089 / 1.089 | 590.567 | 1.152 / 3.551 / 3.551 | 1.242.718 | 888.889 |
| 8.192 × 64 | 2.098.176 | 2.215 / 2.844 / 2.844 | 2.604 | 1.960 / 2.584 / 2.584 | 3.698.420 | 4.179.592 |
| 65.536 × 64 | 16.785.408 | 12.621 / 13.669 / 13.669 | 16.158 | 11.209 / 14.159 / 14.159 | 5.192.615 | 5.846.730 |
| 65.536 × 256 | 67.117.056 | 16.149 / 21.243 / 21.243 | 154.014 | 17.685 / 20.602 / 20.602 | 4.058.208 | 3.705.739 |

Nei dieci dispatch per taglia (uno freddo e nove caldi), i contatori wgpu
hanno registrato rispettivamente transfer/kernel: 2.184/29.859 µs,
1.436/11.240 µs, 5.400/32.307 µs e 17.982/209.618 µs, con nove hit VRAM per
taglia. La latenza end-to-end della tabella include anche sincronizzazione,
readback e top-k.

Il crossover locale non è monotono: la GPU calda supera la CPU nelle due taglie
intermedie, ma non nel batch piccolo né in quello 65.536 × 256. Il modello di
costo deve quindi essere calibrato per adapter e forma del batch; la richiesta
fredda conferma che scegliere GPU usando soltanto il numero di elementi non è
corretto. Questi dati sono diagnostici, non SLA né confronto di prodotto.
