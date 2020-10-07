// 
//----------------------------------------------------------------------------------
//QUERY I. Return the 'position' of the Node with label 'Player'
// 	   and 'name' 'Pierre van Hooijdonk' 
//----------------------------------------------------------------------------------
QUERY-----match(player:Player{name:"Pierre van Hooijdonk"}) 
		  return player.position as position

Explanation : "MATCH" clause is use to search the pattern of a node with its label along with its attributes. In the above lines of
Code Player is a Node label and "name" and "position" is attribute of the label.



-----------------------------------------------------------------------------------
//QUERY II. Return the 'season' and 'fee' for all Nodes with label 'Transfer' and 
//	    having a relationship with the Node with label 'Player' and 'name' 
//	    'Pierre van Hooijdonk'
//-----------------------------------------------------------------------------------
QUERY---MATCH (player:Player{name:"Pierre van Hooijdonk"})--(transfer:Transfer)
		RETURN transfer.season AS season,transfer.fee as fee


Explanation : In the above lines of code the difference appears in the terms of relationship, Here the type of relationship is "outgoing
relationship" since it is directing from player to transfer.


 -------------------------------------------------------------------------------------
// QUERY III. Return the maximum ('max') amount of transfers a Node with label 'Player'
//	      is involved in the dataset. 
//	      Tip: You might consider counting the number of transfers ('count') each
//	           Node with label 'Player' is involved at in order to find the 
//		   maximum among these numbers. 
// -------------------------------------------------------------------------------------
QUERY ----match(transfer:Transfer)-[:OF_PLAYER]->(player:Player)
          with player,count(*) as Total_Transfer
          return max(Total_Transfer) as Max_Count

Explanation: "transfer" and "player" are node name and "Transfer","Player" are node label name of Player and Transfer.
			[:OF_PLAYER] = Relationship between player and transfer
			"with" clause = use to filter the condition (Here bases on player counting the total transfer)
			"max" = find the maximum amount of transfer


 
 -------------------------------------------------------------------------------------
// QUERY IV. Return the 'name' of all Nodes with label 'Player' involved in 7 transfers 
//	     (which we know from QUERY III is the max number of transfers).	     
//	     Collect the results into a list, thus returning just 1 row.
// -------------------------------------------------------------------------------------
QUERY---match(transfer:Transfer)-[:OF_PLAYER]->(player:Player)
		with player,count(*) as Total_Transfer
		where Total_Transfer = 7
		with collect(player.name) as List_of_Players
		return List_of_Players

Explanation : 	"Where" clause = use to add constraints in the code
				"Collect" = Return the output as list


------------------------------------------------------------------------------------------------------
 QUERY V. Return the player 'name' and transfer 'fee' of the most expensive transfer of season 01/02.
// ------------------------------------------------------------------------------------------------------
QUERY---match(transfer:Transfer{season:"01/02"})--(player:Player)
		with transfer,player
		limit 1
		return player.name as name,transfer.fee as fee
			
Explanation--- "limit" clause = constraints for number of rows we want to display in our output.
				Note: I wonder why I did not use "order by" here and came to know the order is already sorted since we can direcly
				apply the limit.


// ------------------------------------------------------------------------------------------------------
// QUERY VI. Return the 'name' of all players transferred to the club 'PSV Eindhoven' in the season 02/03. 	     
// ------------------------------------------------------------------------------------------------------
QUERY---match(player:Player)--(transfer:Transfer{season:"02/03"})--(club:Club)
        with player,transfer,club
        where club.name = "PSV Eindhoven"
        return player.name as player_name



-------------------------------------------------------------------------------------------------------------------
// QUERY VII. Return the number of players transferred from a Spanish to an English club.
// -------------------------------------------------------------------------------------------------------------------
QUERY----match(transfer:Transfer)-[:OF_PLAYER]->(player),
		(from)<-[:FROM_CLUB]-(transfer)-[:TO_CLUB]->(to)
		 where from.country = "Spain" and to.country = "England"
		 return count(*) as count
		
		
Explanation: Note: for English Club I consider Country "England" Here.



// -------------------------------------------------------------------------------------------------------------------
// QUERY VIII. Return the name of the youngest player transferred from 'Real Madrid' to an English club.
// -------------------------------------------------------------------------------------------------------------------
QUERY---match(transfer:Transfer)-[var:OF_PLAYER]->(player),
		(from)<-[:FROM_CLUB]-(transfer)-[:TO_CLUB]->(to)
		where from.name = "Real Madrid" and to.country = "England"
		return player.name,var.age 
		order by var.age
		limit 1



