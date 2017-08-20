class Reviewer:

    name = u''
    id = ""
    location = u''
    title = ""
    numReviews = 0
    numHotelReviews = 0
    helpfulVotes = 0
    
    def __init__(self, name, id, location, title, numReviews, numHotelReviews, helpfulVotes):
        self.name = name
        self.id = id
        self.location = location
        self.title = title
        self.numReviews = numReviews
        self.numHotelReviews = numHotelReviews
        self.helpfulVotes = helpfulVotes
