import calendar, time

t = time.strptime('2017/05/24', '%Y/%m/%d')
epoch = calendar.timegm(time.struct_time(t))
print(epoch)