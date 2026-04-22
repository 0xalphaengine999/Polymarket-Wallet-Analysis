```sh
npx tsx getPositions.ts 0xWALLET
npx tsx getPositions.ts 0xWALLET --last-month
npx tsx getPositions.ts 0xWALLET --last-30d
npx tsx getPositions.ts 0xWALLET --since-days=7
npx tsx getPositions.ts 0xWALLET --since-days=30
npx tsx getPositions.ts 0xWALLET -q
npx tsx getPositions.ts 0xWALLET --quiet
npx tsx getPositions.ts 0xWALLET --last-month -q
npx tsx getPositions.ts 0xWALLET --since-days=7 --quiet
npx tsx getPositions.ts 0xWALLET --since-days=7 --last-month
npx tsx getPositions.ts 0xWALLET --last-month --since-days=7
```

```sh
python analyze.py count WALLET.json
python analyze.py count WALLET.json --5m
python analyze.py count WALLET.json --all
python analyze.py count WALLET.json -o out.png
python analyze.py count WALLET.json --show
python analyze.py count WALLET.json -o out.png --show
python analyze.py count WALLET.json --last-month
python analyze.py count WALLET.json --since-days 7
python analyze.py count WALLET.json --since-days 30
python analyze.py count WALLET.json --5m --last-month
python analyze.py count WALLET.json --5m --since-days 7
python analyze.py count WALLET.json --all --last-month
python analyze.py count WALLET.json --all --since-days 7
python analyze.py count WALLET.json --all -o out.png --show
```

```sh
python analyze.py pnl WALLET.json
python analyze.py pnl WALLET.json --5m
python analyze.py pnl WALLET.json --all
python analyze.py pnl WALLET.json -o out.png
python analyze.py pnl WALLET.json --show
python analyze.py pnl WALLET.json -o out.png --show
python analyze.py pnl WALLET.json --last-month
python analyze.py pnl WALLET.json --since-days 7
python analyze.py pnl WALLET.json --since-days 30
python analyze.py pnl WALLET.json --5m --last-month
python analyze.py pnl WALLET.json --5m --since-days 7
python analyze.py pnl WALLET.json --all --last-month
python analyze.py pnl WALLET.json --all --since-days 7
python analyze.py pnl WALLET.json --all -o out.png --show
```

```sh
python analyze.py daily WALLET.json
python analyze.py daily WALLET.json --5m
python analyze.py daily WALLET.json --all
python analyze.py daily WALLET.json -o out.png
python analyze.py daily WALLET.json --show
python analyze.py daily WALLET.json -o out.png --show
python analyze.py daily WALLET.json --last-month
python analyze.py daily WALLET.json --since-days 7
python analyze.py daily WALLET.json --since-days 30
python analyze.py daily WALLET.json --5m --last-month
python analyze.py daily WALLET.json --5m --since-days 7
python analyze.py daily WALLET.json --all --last-month
python analyze.py daily WALLET.json --all --since-days 7
python analyze.py daily WALLET.json --all -o out.png --show
```
