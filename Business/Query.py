class Query:
    def __init__(self, queryID=None, purpose=None, size=None):
        self.__queryID = queryID
        self.__purpose = purpose
        self.__size = size

    def getQueryID(self):
        return self.__queryID

    def setQueryID(self, queryID):
        self.__queryID = queryID

    def getPurpose(self):
        return self.__purpose

    def setPurpose(self, purpose):
        self.__purpose = purpose

    def getSize(self):
        return self.__size

    def setSize(self, size):
        self.__size = size

    @staticmethod
    def badQuery():
        return Query()

    def __str__(self):
        return "queryID=" + str(self.__queryID) + ", purpose=" + str(self.__purpose) + ", size=" + str(self.__size)
