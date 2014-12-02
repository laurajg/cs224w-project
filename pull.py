from mrjob.job import MRJob
from mrjob.protocol import RawProtocol

from uj import uJSONProtocol, uJSONValueProtocol
from domainparser import domainparser
#from base import InvalidRecord
from app.base import InvalidRecord
from app.fields import rollupObjectFields
from app.fields import rawObjectFields

CONVERTERS = {
    'raw': rawObjectFields,
    'rollup': rollupObjectFields,
    'rollupSmall': rollupSmallObjectFields,
}


def in_time_range(date):
        baseline_start_date = 1396310400  #this is april 1 2014
        baseline_end_date = 1402704000  #this is june 14 2014
        if date < baseline_end_date:
                return date > baseline_start_date
        return date < baseline_end_date

def prepare_item(s):
    if not s:
        return ''
    if not isinstance(s, basestring):
        s = str(s)
    s = s.replace('\t', '\\t')
    s = s.replace('\n', '\\n')
    return s



class pullData(MRJob):

    INPUT_PROTOCOL = uJSONValueProtocol
    OUTPUT_PROTOCOL = uJSONProtocol
    INTERNAL_PROTOCOL = uJSONProtocol

    DOMAIN_PARSER = domainparser.DomainParser()


    def configure_options(self):
        super(pullData, self).configure_options()

        self.add_passthrough_option(
             '--converter',
             dest='converter',
             type='str',
             default='rollup',
        )
        self.add_file_option('--tlds')


    def load_options(self, args):
        super(pullData, self).load_options(args)

        converter = self.options.converter
        converter_cls = CONVERTERS.get(converter)

        if converter_cls is None:
            raise NotImplementedError('There is no %s converter' % converter)

        self.converter_cls = converter_cls


    def mapper_init(self):
        self.dp = domainparser.DomainParser(self.options.tlds)
        self.converter = self.converter_cls(self.dp)


def mapper(self, _, item):

        if '_heartbeat_' not in item:

            self.increment_counter('pull_data', 'item_processed', 1)
            if in_time_range(item['hc']) and ("nytimes" in item['u']):
                try:
                        values = self.converter.convert(item)
                        values_str = " ".join([prepare_item(v) for v in values])
                        output = "%s: %s" % (item['g'], values_str)
                        yield output, 1
                except InvalidRecord:
                        self.increment_counter('data_pull', 'invalid_records', 1)
                        return
                except UnicodeEncodeError:
                        self.increment_counter('data_pull', 'UnicodeEncodeError', 1)
                        return
                except UnicodeDecodeError:
                        self.increment_counter('data_pull', 'UnicodeDecodeError', 1)
                        return




    def reducer(self, key, values):
        yield(key, sum(values))


  #  def steps(self):

#       return([self.mr(mapper=self.mapper, combiner=self.reducer,
 #                       reducer=self.reducer)])

if __name__ == '__main__':
    pullData.run()

