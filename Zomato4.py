from mrjob.job import MRJob
from mrjob.step import MRStep
class Zomato(MRJob):
	def steps(self):
		return[
			MRStep(mapper=self.mymapper,reducer=self.myreducer)
			]
	def mymapper(self,_,line):
		field={}
		field=line.split(',')
		rt=field[8]
		yield rt,1
		
	def myreducer(self,key,value):
		yield key,sum(value)
if __name__=='__main__':
	Zomato.run()


