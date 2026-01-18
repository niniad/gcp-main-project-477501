#!/usr/bin/env python3
"""
BigQuery データセット、テーブル、カラム情報を調査するスクリプト
"""

from google.cloud import bigquery
import json
import os

def main():
    # プロジェクトID
    project_id = "main-project-477501"

    # 認証情報のパスを設定
    credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if not credentials_path:
        print("エラー: GOOGLE_APPLICATION_CREDENTIALS環境変数が設定されていません。")
        return

    # BigQueryクライアントの作成
    client = bigquery.Client(project=project_id)

    print(f"プロジェクト: {project_id}")
    print("=" * 80)

    # すべてのデータセットを取得
    datasets = list(client.list_datasets())

    if not datasets:
        print("\n⚠️  このプロジェクトにはデータセットが存在しません。")
        print("\n【推奨】EC事業者向けにデータセットを新規作成することを提案します。")
        return

    print(f"\n📊 データセット数: {len(datasets)}")
    print("=" * 80)

    all_data = {}

    # 各データセットについて調査
    for dataset in datasets:
        dataset_id = dataset.dataset_id
        print(f"\n\n🗂️  データセット: {dataset_id}")
        print("-" * 80)

        dataset_ref = client.dataset(dataset_id)
        tables = list(client.list_tables(dataset_id))

        if not tables:
            print("  テーブルなし")
            continue

        print(f"  📋 テーブル数: {len(tables)}")

        dataset_info = {
            "dataset_id": dataset_id,
            "tables": []
        }

        # 各テーブルについて調査
        for table in tables:
            table_id = table.table_id
            table_ref = dataset_ref.table(table_id)
            table_obj = client.get_table(table_ref)

            print(f"\n  テーブル: {table_id}")
            print(f"    - 行数: {table_obj.num_rows:,}")
            print(f"    - サイズ: {table_obj.num_bytes / (1024*1024):.2f} MB")
            print(f"    - 作成日: {table_obj.created}")
            print(f"    - 更新日: {table_obj.modified}")

            # スキーマ（カラム情報）を取得
            print(f"    - カラム数: {len(table_obj.schema)}")
            print(f"    - カラム:")

            columns = []
            for field in table_obj.schema:
                mode = f" ({field.mode})" if field.mode != "NULLABLE" else ""
                description = f" - {field.description}" if field.description else ""
                print(f"      • {field.name}: {field.field_type}{mode}{description}")

                columns.append({
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode,
                    "description": field.description
                })

            # サンプルデータを取得（最初の3行）
            query = f"""
                SELECT *
                FROM `{project_id}.{dataset_id}.{table_id}`
                LIMIT 3
            """

            try:
                query_job = client.query(query)
                results = query_job.result()

                print(f"    - サンプルデータ（最初の3行）:")
                for i, row in enumerate(results, 1):
                    print(f"      Row {i}: {dict(row)}")
            except Exception as e:
                print(f"    - サンプルデータ取得エラー: {e}")

            table_info = {
                "table_id": table_id,
                "num_rows": table_obj.num_rows,
                "size_mb": table_obj.num_bytes / (1024*1024),
                "created": str(table_obj.created),
                "modified": str(table_obj.modified),
                "columns": columns
            }

            dataset_info["tables"].append(table_info)

        all_data[dataset_id] = dataset_info

    # JSON形式でも出力
    print("\n\n" + "=" * 80)
    print("📄 詳細情報（JSON形式）")
    print("=" * 80)
    print(json.dumps(all_data, indent=2, ensure_ascii=False))

    # 結果をファイルに保存
    output_file = "/home/user/gcp-main-project-477501/bigquery_structure.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)

    print(f"\n\n✓ 詳細情報を {output_file} に保存しました")

if __name__ == "__main__":
    main()
