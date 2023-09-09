package recipes.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import provided.util.DaoBase;
import recipes.entity.Recipe;
import recipes.exception.DbException;

public class RecipeDao extends DaoBase {
  public void executeBatch(List<String> sqlBatch) {
	  try(Connection conn = DbConnection.getConnection()) {
		  startTransaction(conn);
	
		try(Statement stmt = conn.createStatement()) {
		for(String sql : sqlBatch ) {
			stmt.addBatch(sql);
		}
		stmt.executeBatch();
		commitTransaction(conn);
	}
	catch(Exception e) {
	rollbackTransaction(conn);
	throw new DbException(e);
	}
  } catch (SQLException e) {
	  throw new DbException(e);
  }
  }

public List<Recipe> fetchAllRecipes() {
	String sql = "SELECT * FROM " + RECIPE_TABLE + " ORDER BY recipe_name";
	
	try(Connection conn = DbConnection.getConnection()) {
		startTransaction(conn);
		
		try(PreparedStatement stmt = conn.prepareStatement(sql)) {
		try(ResultSet rs = stmt.executeQuery()) {
			List<Recipe> recipes = new LinkedList<>();
			
			while(rs.next()) {
				recipes.add(extract(rs, Recipe.class));
			}
			
			return recipes;
		}
		}
		catch(Exception e) {
			rollbackTransaction(conn);
			throw new DbException(e);
		}
	} catch (SQLException e) {
		throw new DbException(e);
	}
	}
}

