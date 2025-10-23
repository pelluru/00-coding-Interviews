class Trie:
    def __init__(self): self.ch={}; self.end=False
    def insert(self,w):
        node=self
        for c in w: node=node.ch.setdefault(c, Trie())
        node.end=True
    def search(self,w):
        node=self
        for c in w:
            if c not in node.ch: return False
            node=node.ch[c]
        return node.end
    def startsWith(self,p):
        node=self
        for c in p:
            if c not in node.ch: return False
            node=node.ch[c]
        return True

def replace_words(roots, sentence):
    trie=Trie()
    for r in roots: trie.insert(r)
    def repl(word):
        node=trie; pref=''
        for c in word:
            if c not in node.ch: return word
            node=node.ch[c]; pref+=c
            if node.end: return pref
        return word
    return " ".join(repl(w) for w in sentence.split())

def word_break(s, words):
    trie=Trie()
    for w in words: trie.insert(w)
    n=len(s); dp=[False]*(n+1); dp[0]=True
    for i in range(n):
        if not dp[i]: continue
        node=trie
        for j in range(i, n):
            c=s[j]
            if c not in node.ch: break
            node=node.ch[c]
            if node.end: dp[j+1]=True
    return dp[n]
