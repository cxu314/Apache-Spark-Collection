use msan697
//Drop the field name “restaurant_id”. 
db.business.update({},{$unset:{"restaurant_id":1}},{multi:1})
//Change field name “borough” to “district”. 
db.business.update({},{$rename:{"borough":"district"}},{multi:1})
//Find a business which “name” is " Kunjip Restaurant" (Indented format). 
db.business.findOne({"name":"Kunjip Restaurant"})
//Drop the “business” collection. 
db.business.drop()
