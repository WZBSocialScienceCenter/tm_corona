to_kalliope:
	rsync -rcv --exclude-from=.rsyncexclude . kalliope:~/Documents/tm_corona/

from_kalliope:
	rsync -rcv --exclude-from=.rsyncexclude kalliope:~/Documents/tm_corona/ .

