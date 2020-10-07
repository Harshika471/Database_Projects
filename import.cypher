//
load csv with headers from "file:///transfers.csv" as row
return count(*)
//
create constraint on (player:Player)
assert player.id is unique
//
load csv with headers from "file:///transfers.csv" as row
merge (player:Player {id:row.playerUri})
on create set player.name = row.playerName, player.position = row.playerPosition
//
create constraint on (club:Club)
assert club.id is unique
//
load csv with headers from "file:///transfers.csv" as row
merge (club:Club {id:row.sellerClubUri})
on create set club.name = row.sellerClubName, club.country = row.sellerClubCountry
//
load csv with headers from "file:///transfers.csv" as row
merge (club:Club {id:row.buyerClubUri})
on create set club.name = row.buyerClubName, club.country = row.buyerClubCountry
//
create constraint on (transfer:Transfer)
assert transfer.id is unique
//
load csv with headers from "file:///transfers.csv" as row
match (player:Player {id:row.playerUri})
match (source:Club {id:row.sellerClubUri})
match (destination:Club {id:row.buyerClubUri})
merge (t:Transfer {id: row.transferUri})
on create set t.season = row.season, t.rank = row.transferRank, t.fee = row.transferFee
merge (t)-[:OF_PLAYER {age:row.playerAge}]->(player)
merge (t)-[:FROM_CLUB]->(source)
merge (t)-[:TO_CLUB]->(destination)
//
match (player:Player {name:"Cristiano Ronaldo"}) return player

