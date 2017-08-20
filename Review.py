class Review:
    
    id = ""
    product = ""
    name = ""
    dt = 0
    stars = ""
    text = ""
    reviewerID = ""
    prtnrCollected = 0
    helpfulCnt = 0
    starsNum = 0
    polarity = 0
    
    def __init__(self, id, product, name, dt, stars, text, reviewerID, prtnrCollected, helpfulCnt, starsNum, polarity):
        self.id = id
        self.product = product
        self.name = name
        self.dt = dt
        self.stars = stars
        self.text = text
        self.reviewerID = reviewerID
        self.prtnrCollected = prtnrCollected         
        self.helpfulCnt = helpfulCnt
        self.starsNum = starsNum
        self.polarity = polarity
