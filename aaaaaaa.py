import spacy

file_pass = "n2443gu.txt"

with open(file_pass) as f:
    text = f.read()

nlp = spacy.load('ja_ginza')
doc = nlp(text)

for tok in doc:
    print(
        tok.i,  # 親ドキュメント内のトークンのインデックス。
        tok.text,  # 逐語的なテキストコンテンツ

        # 逐語的なテキストコンテンツ（Token.textと同じ）
        # 主に他の属性との一貫性のために存在します。
        tok.orth_,

        tok.lemma_,  # 語尾変化のない接尾辞のない、トークンの基本形式
        tok.pos_,  # 品詞（英語の大文字）
        tok.tag_,  # きめの細かい品詞
        tok.head.i, # headはこのトークンの構文上の親、または「ガバナー」
        tok.dep_,  # 構文従属関係

        # トークンの基準、つまりトークンテキストの正規化された形式
        # 通常、言語のトークナイザー例外またはノルム例外で設定されます。
        tok.norm_,

        # 名前付きエンティティタグのIOBコード
        # 「B」はトークンがエンティティを開始することを意味し、「I」はそれがエンティティの内部にあることを意味し、
        # 「O」はそれがエンティティの外部にあることを意味し、「」はエンティティタグが設定されていないことを意味します。
        tok.ent_iob_,
        tok.ent_type_,  # 名前付きエンティティタイプ
    )