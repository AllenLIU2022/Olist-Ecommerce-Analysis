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
* **物流の逆説:** 分析の結果、遠隔地の方が都市部よりも公式な遅延率が低いという逆説的な事実が判明しました。
* **セラーエコシステム:** パレート図（80/20の法則）を用いて、特定のセラーに依存しない健全なロングテール市場であることを証明。
* **分割払いの戦略分析:** 全注文の50%以上で分割払いが利用されており、特に10回払いに顕著なピークが見られます。
* (戦略的提言を含む詳細な分析については、メインREADMEをご参照ください。)

## 📂 ディレクトリ構成
* `notebooks/`: データクレンジング、型変換（PySpark）
* `sql/`: 論理データウェアハウス構築、ビジネス分析クエリ
