# Benchmark compute Milestone 6

Il laboratorio misura l'operatore vector exact/top-k CPU e wgpu sugli stessi
batch colonnari e verifica il ranking entro tolleranza relativa `1e-4`.

```powershell
cargo run --release -p aprodb-compute --features gpu --example compute_crossover
```

Per ogni dimensione esegue nove campioni CPU, una richiesta GPU fredda e nove
richieste con proiezione già in VRAM. La latenza GPU end-to-end include upload,
dispatch, sincronizzazione, readback e top-k; i contatori separano inoltre
tempo di trasferimento e kernel. I dati sono deterministici. I risultati locali
non sono SLA e il crossover va ricalibrato sull'hardware di destinazione.

I risultati verificati sono in [RESULTS.md](RESULTS.md).
