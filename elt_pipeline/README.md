# Создание ELT пайплайна для Airflow 

Задача: написать DAG в airflow, который будет считаться каждый день за вчера. 

Название среза - dimension
Значение среза - dimension_value
Число просмотров - views
Число лайков - likes
Число полученных сообщений - messages_received
Число отправленных сообщений - messages_sent
От скольких пользователей получили сообщения - users_received
Скольким пользователям отправили сообщение - users_sent
Срезы — os, gender и age

Каждый день таблица в ClickHouse должна дополняться новыми данными.

<br><br><br>

Авторство задания принадлежит Karpov.Courses  
Курс Симулятор аналитика: https://karpov.courses/simulator 