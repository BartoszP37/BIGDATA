from mrjob.job import MRJob

class mapreduce(MRJob):
    def mapper(self, _, line):
        fields=line.split(",")
        if len(fields)==4:
            (userId, movieId, rating ,timestamp)=fields
            if movieId !='movieId':
                result=(movieId,('R',rating))
                yield result
        elif len(fields)==3:
            (movieId, title, genres)=fields
            if movieId !='movieId':
                result=(movieId, ('T', title))
                yield result

    def reducer(self, movieId, values):
        ratings = []
        title = None

        for vtype, value in values:
            if vtype == 'R':
                try:
                    rating = float(value)
                    ratings.append(rating)
                except ValueError:
                    pass
            elif vtype == 'T':
                title = value

        if ratings:
            average_rating = sum(ratings) / len(ratings)
            yield title, average_rating

if __name__ == '__main__':
    mapreduce.run()





