from mrjob.job import MRJob

class MRTotalQuantity(MRJob):

    def mapper(self, _, line):
        # Split line into product and quantity
        product, qty = line.split(',')
        yield product.strip().lower(), int(qty.strip())

    def reducer(self, product, qtys):
        yield product, sum(qtys)

if __name__ == '__main__':
    MRTotalQuantity.run()
