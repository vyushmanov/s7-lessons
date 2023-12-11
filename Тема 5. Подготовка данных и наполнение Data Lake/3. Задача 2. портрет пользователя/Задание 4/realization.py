# calculate_user_interests('2022-05-31', 7, spark).write.mode("overwrite").parquet(f'{host}/user/vyushmanov/data/analytics/interests_d7')
#
# calculate_user_interests('2022-05-31', 28, spark).write.mode("overwrite").parquet(f'{host}/user/vyushmanov/data/analytics/interests_d28')


def compare_df(file, file_example):
    return file.count() == file_example.count()
