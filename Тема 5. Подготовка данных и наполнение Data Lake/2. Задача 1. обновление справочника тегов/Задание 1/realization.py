from datetime import datetime, timedelta

def input_paths(date, depth):
    path_list =[]
    for i in range(depth):
        start_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=-i)
        start_date = datetime.strftime(start_date,'%Y-%m-%d')
        path_list.append(f'/user/vyushmanov/data/events/date={start_date}/event_type=message')
    return path_list