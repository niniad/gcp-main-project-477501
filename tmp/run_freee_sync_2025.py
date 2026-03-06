"""freee FY2025 同期をy確認なしで直接実行するラッパー"""
import sys, builtins
sys.stdout.reconfigure(encoding='utf-8')

# input()をモンキーパッチしてyを返す
builtins.input = lambda prompt='': 'y'

# スクリプトを直接実行
exec(open('tmp/freee_sync_fy2025.py', encoding='utf-8').read())
