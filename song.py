class song:
    id=0
    year=""
    genre=""
    shingle_list=[]
    similar_songs=[]
    similarity_score=0
    signature=""
    n=0

    def __init__(self,id,signature):
        self.id=id
        # self.genre=genre
        # self.year=year
        # self.shingle_list=shingles
        self.signature=signature.split(",")

    def assign_similar_songs(self,similars):
        self.similar_songs=similars

    def printer(self):
        print self.id
        # print self.similar_songs,"similar_songs"
        # print self.signature,"signature"
        # print self.shingle_list,"shingle list"
        # print self.shingle_list

    def __eq__(self,other):
        return self.id==other.id

