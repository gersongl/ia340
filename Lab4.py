#!/usr/bin/env python
# coding: utf-8

# In[120]:


import pandas
import configparser
import psycopg2


# In[121]:


config = configparser.ConfigParser()
config.read('config.ini')

host=config['myaws']['host']
db=config['myaws']['db']
user=config['myaws']['user']
pwd=config['myaws']['pwd']


# In[122]:


conn = psycopg2.connect(host=host,
                       user=user,
                       password=pwd,
                       dbname=db)


# In[73]:


sql = " select * from student "


# In[74]:


df = pandas.read_sql_query(sql,conn)
df[:]


# In[75]:


sql_1 = """ 
        select professor.p_name, course.c_name
        from professor
        inner join course
        on professor.p_email = course.p_email
"""


# In[76]:


df = pandas.read_sql_query(sql_1,conn)
df[:]


# In[77]:


sql_2 = """
select count(*) as num_stu, c_number
from enrollment
group by c_number
"""


# In[78]:


df = pandas.read_sql_query(sql_2,conn)
df.plot.bar(x='c_number',y='num_stu')


# In[79]:


sql_3 = """
select count(c_number) as course_count, p_name
from professor
left join course
on professor.p_email = course.p_email
group by p_name
"""


# In[80]:


df = pandas.read_sql_query(sql_3,conn)
df.plot.bar(x = 'p_name', y = 'course_count')


# In[81]:


sql_4 = """
insert into student(s_email,s_name,major)
values('{}','{}','{}')
""".format('s5@jmu.edu','s5','GS')

print(sql_4)


# In[124]:


cur = conn.cursor()


# In[84]:


cur.execute(sql_4)


# In[85]:


conn.commit()


# In[86]:


df = pandas.read_sql_query('select * from student',conn)
df[:]


# In[87]:


sql_5 = """
insert into professor(p_email,p_name,office)
values('{}','{}','{}')
""".format('p4@jmu.edu','p4','o4')

print(sql_5)


# In[88]:


cur.execute(sql_5)


# In[89]:


conn.commit()


# In[90]:


conn.rollback()


# In[91]:


sql_6 = """select * from professor"""
print(sql_6)


# In[92]:


df = pandas.read_sql_query(sql_6,conn)
df[:]


# In[95]:


sql_7 = """
update course
set p_email = '{}'
where p_email = '{}'
""".format('p4@jmu.edu','p2@jmu.edu')

print(sql_7)


# In[104]:


cur.execute(sql_7)


# In[105]:


conn.commit()


# In[106]:


sql_8 = """select * from course"""
print(sql_8)


# In[107]:


df = pandas.read_sql_query(sql_8,conn)
df[:]


# In[109]:


sql_9 = """
delete from professor
where p_email = '{}'
""".format('p2@jmu.edu')

print(sql_9)


# In[125]:


cur.execute(sql_9)


# In[126]:


conn.commit()


# In[127]:


sql_10 = """select * from professor"""
print(sql_10)


# In[129]:


df = pandas.read_sql_query(sql_10,conn)
df[:]


# In[ ]:




