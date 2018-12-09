###Stage-2: 

PUT line — https://overload.yandex.net/139795 

PUT line+const — https://overload.yandex.net/139812 

GET line — https://overload.yandex.net/139799 

GET line+const — https://overload.yandex.net/139816 

PUT+GET line — https://overload.yandex.net/139801 

PUT+GET line+const — https://overload.yandex.net/139819 

###Stage-3: 
PUT line — https://overload.yandex.net/139804 

PUT line+const — https://overload.yandex.net/139821 

GET line — https://overload.yandex.net/139805 

GET line+const — https://overload.yandex.net/139823 

PUT+GET line — https://overload.yandex.net/139806 

PUT+GET line+const — https://overload.yandex.net/139827 

JFR - https://drive.google.com/open?id=1puIRrxLj-afTPEMLeyTJlMla7LZdf9YE

После оптимизаций(Stage-3) удалось взять на 1000 rps больше. 
Ухудшилась пиковая пропускная способность(логично т.к. уменьшилось кол во потоков на каждой ноде), однако стабильность(на большей постоянной нагрузке) явно лучше. Есть ощущение, что оптимизации дали буст, но я не стал пробовать брать еще больше rps.
