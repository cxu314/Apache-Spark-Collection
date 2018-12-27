//FILL THIS .JS FILE.
use msan697
db.business.update({},{$rename:{"cuisine":"cuisine_type"}}, {multi:true})
db.business.findOne({"name":"1 East 66Th Street Kitchen"})