from mrjob.job import MRJob

class mapreduce(MRJob):
    def mapper(self, _, line):
        userId, movieId, rating, timestamp = line.split(",")
        try:
            rating = float(rating)
            yield movieId, (rating, 1)  
        except ValueError:
            pass  

    def reducer(self, movieId, rating_count_pairs):
        total_rating, total_count = 0, 0
        for rating, count in rating_count_pairs:
            total_rating += rating
            total_count += count
        average_rating = total_rating / total_count if total_count > 0 else 0
        yield movieId, average_rating  

if __name__ == '__main__':
    mapreduce.run()



