#!/usr/bin/env python3
"""
BigQuery データセット構造のサマリーを作成
"""

import json

def main():
    # JSONファイルを読み込む
    with open("/home/user/gcp-main-project-477501/bigquery_structure.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    print("=" * 80)
    print("BigQuery データ構造サマリー")
    print("=" * 80)
    print(f"\nデータセット数: {len(data)}\n")

    for dataset_id, dataset_info in data.items():
        print(f"\n{'='*80}")
        print(f"📊 データセット: {dataset_id}")
        print(f"{'='*80}")
        print(f"  テーブル数: {len(dataset_info['tables'])}")

        for table in dataset_info['tables']:
            print(f"\n  📋 テーブル: {table['table_id']}")
            print(f"     - 行数: {table['num_rows']:,}")
            print(f"     - サイズ: {table['size_mb']:.2f} MB")
            print(f"     - カラム数: {len(table['columns'])}")
            print(f"     - 主要カラム:")
            for col in table['columns'][:10]:  # 最初の10カラムのみ
                print(f"       • {col['name']}: {col['type']}")
            if len(table['columns']) > 10:
                print(f"       ... 他 {len(table['columns']) - 10} カラム")

    print("\n" + "=" * 80)
    print("データセット別テーブル一覧")
    print("=" * 80)
    for dataset_id, dataset_info in data.items():
        table_names = [t['table_id'] for t in dataset_info['tables']]
        print(f"\n{dataset_id}:")
        for i, name in enumerate(table_names, 1):
            print(f"  {i}. {name}")

if __name__ == "__main__":
    main()
