# 🇧🇷 Olist E-commerce データ分析プロジェクト
**言語 / Language :** [日本語](README_JP.md) | [English](README.md) | [Français](README_FR.md)

## 📌 プロジェクト概要
ブラジルの大手マーケットプレイス「Olist」のデータセットを用いた、エンドツーエンドのデータエンジニアリング及びビジネス分析プロジェクトです。
**Azure Synapse Analytics** 上で **Medallion Architecture** を構築し、PySpark と SQL を活用して物流最適化や販売者パフォーマンスの可視化を行いました。

## 🛠️ 技術スタック (Tech Stack)
* **データ基盤:** Azure Data Lake Storage (ADLS) Gen2
* **処理エンジン:** PySpark (Azure Synapse Spark Pool)
* **アーキテクチャ:** メダリオン構造 (Bronze -> Silver -> Gold)
* **分析レイヤー:** Serverless SQL (Azure Synapse)

## 💡 ビジネスインサイト (Key Insights)
* **物流効率の最適化:** 州別の配送リードタイムを分析し、配送遅延率の高い地域に対してフルフィルメントセンター（前置倉）の設置を提案。
* **セラーエコシステム:** パレート図（80/20の法則）を用いて、特定のセラーに依存しない健全なロングテール市場であることを証明。
* **顧客満足度分析:** 分割払いの回数とレビュースコアの相関関係を分析し、支払い負担感による顧客満足度低下の防止策を検討。
* (戦略的提言を含む詳細な分析については、メインREADMEをご参照ください。)

## 📂 ディレクトリ構成
* `notebooks/`: データクレンジング、型変換（PySpark）
* `sql/`: 論理データウェアハウス構築、ビジネス分析クエリ
