import spacy
from spacy import displacy
nlp = spacy.load('ja_ginza')

#テキスト読み込み
filepath = "n2443gu.txt"

with open(filepath) as f:
    s = f.read()

#ここでいろんな処理が行われる
doc = nlp(s)

#固有表現抽出の結果の描画
displacy.render(doc, style="ent", jupyter=True)