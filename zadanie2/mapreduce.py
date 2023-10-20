from mrjob.job import MRJob

class mapreduce(MRJob):
    def mapper(self, _, line):
        if not hasattr(self, 'movie_titles'):
            self.movie_titles = {}
            with open('C:/Users/Bartosz/Desktop/zadanie2/movies.csv', 'r', encoding='latin-1') as movies_file:
                for movie_line in movies_file:
                    movieId, title, genres = movie_line.strip().split(',', 2)
                    self.movie_titles[movieId] = title

        userId, movieId, rating, timestamp = line.split(",")
        try:
            rating = float(rating)
            if movieId in self.movie_titles:
                movie_title = self.movie_titles[movieId]
                yield movie_title, (rating, 1)
        except ValueError:
            pass

    def reducer(self, movie_title, rating_count_pairs):
        total_rating, total_count = 0, 0
        for rating, count in rating_count_pairs:
            total_rating += rating
            total_count += count
        average_rating = total_rating / total_count if total_count > 0 else 0
        yield movie_title, average_rating

if __name__ == '__main__':
    mapreduce.run()





