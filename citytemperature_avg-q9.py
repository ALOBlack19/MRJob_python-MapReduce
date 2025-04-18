from mrjob.job import MRJob

class MRCityAvgTemp(MRJob):

    def mapper(self, _, line):
        # Split line: city, temperature
        city, temp = line.split(',')
        yield city.strip(), (int(temp.strip()), 1)  # temp, count

    def reducer(self, city, values):
        total_temp, count = 0, 0
        for temp, c in values:
            total_temp += temp
            count += c
        # Calculate average
        yield city, total_temp / count

if __name__ == '__main__':
    MRCityAvgTemp.run()