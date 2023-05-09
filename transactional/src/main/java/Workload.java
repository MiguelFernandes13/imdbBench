import com.sun.source.tree.ParenthesizedPatternTree;
import model.Person;
import model.Title;
import model.TitleInfo;

import java.sql.*;
import java.util.*;

public class Workload {
    private final Random rand;
    private final Connection conn;
    private final int nUsers;
    private final List<String> titleTypes;
    private final PreparedStatement getTitleFromPreference;
    private final PreparedStatement getTitleFromType;
    private final PreparedStatement getTitleFromPopular;
    private final PreparedStatement addTitleToUserList;
    private final PreparedStatement getUserWatchList;
    private final PreparedStatement getTitleInfo;
    private final PreparedStatement getTitleMainCastAndCrew;
    private final PreparedStatement getUserRegion;
    private final PreparedStatement getTitleNameInRegion;
    private final PreparedStatement addTitleToHistory;
    private final PreparedStatement removeTitleFromWatchList;
    private final PreparedStatement rateTitleFromHistory;
    private final PreparedStatement getTitleRating;
    private final PreparedStatement searchTitleByName;
    // (note: the list of words to search is static to simplify the benchmark.
    // however, assume that any expression could be given.)
    private final List<String> searchWords = Arrays.asList(
        "north", "love", "south", "blue", "iron", "eye", "city", "home", "heart", "lady", "man", "year",
        "dream", "side", "wine", "sunday", "king", "sea", "battle", "gold", "blade", "ghost", "old",
        "wall", "red", "first", "woman", "queen", "castle", "house", "west", "angel", "super", "time");


    public Workload(Connection c) throws Exception {
        this.rand = new Random();
        this.conn = c;
        this.conn.setAutoCommit(false); // autocommit = off to execute operations inside a transaction
        Statement s = c.createStatement();

        var rs = s.executeQuery("select count(*) from users");
        rs.next();
        this.nUsers = rs.getInt(1);

        // load title types
        this.titleTypes = new ArrayList<>();
        rs = s.executeQuery("select distinct title_type from title");
        while (rs.next()) {
            titleTypes.add(rs.getString(1));
        }

        /* newTitleToList */
        
        //SELECT COUNT(DISTINCT genre_id) as genre_id_distinct_count, COUNT(DISTINCT title_id) as title_id_distinct_count FROM titleGenre;
        // Result:
        //genre_id_distinct_count | title_id_distinct_count
        //-------------------------+-------------------------
        //                      28 |                 9327824
        //create index on titleGenre using btree(title_id, genre_id); já existe

        //SELECT COUNT(DISTINCT genre_id) as genre_id_distinct_count, COUNT(DISTINCT user_id) as user_id_distinct_count FROM userGenre;
        // Result:
        //genre_id_distinct_count | user_id_distinct_count
        //-------------------------+------------------------
        //                      28 |                 100000
        //create index on userGenre using btree(user_id, genre_id); já existe

        //create index on userList using hash(user_id); não utiliza, prefere btree
        //create index on userHistory using hash(user_id); não utiliza, prefere btree

        //create index on usergenre using hash (user_id ); melhorou o tempo de execucao passou de seq scan para index scan
        //nenhum dos indices acima melhorou o tempo de execucao
        //user_id = '500' and '100000'
        this.getTitleFromPreference = c.prepareStatement("""
            select tg.title_id
            from titleGenre tg
            join userGenre ug on tg.genre_id = ug.genre_id
            where ug.user_id = ?
                and tg.title_id not in (
                    select title_id
                    from (
                        select title_id, user_id
                        from userList
                        union
                        select title_id, user_id
                        from userHistory
                    ) t_
                    where t_.user_id = ?
                )
            limit 1;
        """);

        // select count(distinct title_type) from title;
        // Result: 11
        // Como só existem 11 tipos de filmes
        // create index on title using btree(start_year, title_type); (melhorou o tempo de execucao)
        // create index on title using btree(start_year desc, title_type); obteve o melhor resultado

        //user_id = '100000' and title_type = 'movie'
        this.getTitleFromType = c.prepareStatement("""
            select t.id
            from title t
            where t.title_type = ?
                and start_year <= date_part('year', now())
                and t.id not in (
                    select title_id
                    from (
                        select title_id, user_id
                        from userList
                        union
                        select title_id, user_id
                        from userHistory
                    ) t_
                    where t_.user_id = ?
                )
            order by start_year desc
            limit 1;
        """);

        //create index on userHistory using btree(last_seen);
        //criei com hash e btree e escolheu btree
        //create index on userHistory using btree(last_seen desc); obteve o melhor resultado
        //create index on userhistory using hash(last_seen); para o group by porem nao utilizou

        //create index on userhistory using btree(last_seen, title_id); melhorou
        //por causa do index only scan backward
        //create index on userhistory using btree(last_seen desc, title_id); obteve o melhor resultado
        //user_id = '100000'
        this.getTitleFromPopular = c.prepareStatement("""
            select title_id
            from (
                select title_id
                from userHistory
                where last_seen between now()::date - interval '7 days' and now()::date - interval '1 day'
                    and title_id not in (
                        select title_id
                        from (
                            select title_id, user_id
                            from userList
                            union
                            select title_id, user_id
                            from userHistory
                        ) t_
                        where t_.user_id = ?
                    )
                order by last_seen desc
                limit 10000
            ) t_
            group by title_id
            order by count(*) desc
            limit 1
        """);

        this.addTitleToUserList = c.prepareStatement("""
            insert into userList values (?, ?, now())
        """);

        /* getWatchListInformation */

        //create index on userList using btree(user_id); ja existe btree(user_id, title_id)
        //user_id = '100000'
        this.getUserWatchList = c.prepareStatement("""
            select title_id, created_date
            from userList
            where user_id = ?
            order by created_date desc
        """);

        //id = 'tt2386381'
        //com hash obtem um resultado nao significativamente melhor logo nao compensa
        this.getTitleInfo = c.prepareStatement("""
            select primary_title, title_type, coalesce(runtime_minutes, 60), start_year
            from title
            where id = ?
        """);

        //create index on titlePrincipals using hash(name_id); prefere o btree(title_id, ordering)
        //create index on titlePrincipals using hash(category_id); prefere o btree(category_id)
        //create index on titlePrincipalsCharacters using hash(name_id); prefere o btree(title_id, name_id, name)
        //create index on name using hash(id); melhorou
        //create index on category using hash(id); nao melhorou

        //SELECT COUNT(DISTINCT title_id) as title_id_distinct_count, COUNT(DISTINCT name_id) as name_id_distinct_count FROM titlePrincipalsCharacters;
        // Result:  
        // title_id_distinct_count | name_id_distinct_count
        //-------------------------+------------------------
        //                 6985782 |                2880382
        //
        //Since the number of distinct title_id is much larger than the number of distinct name_id, we can use the composite index on (title_id, name_id) to speed up the query.
        //create index on titlePrincipalsCharacters using btree(title_id, name_id); (já existe)
        //get the title_id most frequent in titlePrincipal
        //select title_id, count(*) from titlePrincipals group by title_id order by count(*) desc limit 1;
        
        //get the title_id most frequent in titlePrincipalCharacters
        //select title_id, count(*) from titlePrincipalsCharacters group by title_id order by count(*) desc limit 1;
        // Result:  tt0041024 |    30
        // tt2872750

        //Foi removido o ordering do select uma vez que nao é necessario
        this.getTitleMainCastAndCrew = c.prepareStatement("""
            select ordering, n.id, n.primary_name, c.name, pc.name
            from titlePrincipals p
            join name n on n.id = p.name_id
            join category c on c.id = p.category_id
            left join titlePrincipalsCharacters pc on pc.title_id = p.title_id and pc.name_id = p.name_id
            where p.title_id = ?
            order by ordering;
        """);

        //create index on users using hash(id); (já existe)
        //id = ' 100000'
        this.getUserRegion = c.prepareStatement("""
            select country_code
            from users
            where id = ?
        """);

        //SELECT COUNT(DISTINCT title_id) as title_id_distinct_count, COUNT(DISTINCT region) as region_distinct_count FROM titleAkas;
        // Result:
        //title_id_distinct_count | region_distinct_count
        //-------------------------+-----------------------
        //                 7007800 |                   248
        //Create index on titleAkas using btree(title_id, region);
        //é um indice que nao vale a pena criar uma vez que o numero de regioes diferentes por title_id é muito pequeno
        //logo o overhead criado pelo indice nao compensa

        //select title_id, count(distinct region) from titleAkas group by title_id order by count(distinct region) desc limit 20;
        //  title_id  | count
        //------------+-------
        // tt2872750  |   104
        // tt1067106  |   101
        // tt0088814  |    87
        // tt0056869  |    86
        // tt7014378  |    86
        // tt8052676  |    83
        // tt10687202 |    76
        // tt10696784 |    75
        // tt5113044  |    75
        // tt0099785  |    74
        // tt1430626  |    74
        // tt0090605  |    73
        // tt6751668  |    73
        // tt14024906 |    73
        // tt8185052  |    72
        // tt15863594 |    71
        // tt0068646  |    70
        // tt6932874  |    69
        // tt2850386  |    68
        // tt0060196  |    68
        this.getTitleNameInRegion = c.prepareStatement("""
            select title
            from titleAkas
            where title_id = ?
                and region = ?
        """);

        /* viewTitle */

        //create index on userList using hash(title_id); 
        //create index on title using hash(id); (already created)
        //create index on userList using hash(user_id); prefere o btree(user_id, title_id)

        //user_id = '100000' and least = 20
        this.addTitleToHistory = c.prepareStatement("""
            with selected as (
                select title_id, coalesce(runtime_minutes, 60) as runtime_minutes, user_id
                from userList
                join title on title_id = title.id
                where user_id = ?
                order by random()
                limit 1
            )
            insert into userHistory
            select user_id, title_id, least(?, selected.runtime_minutes), now(), now(), null
            from selected
            on conflict (user_id, title_id)
            do update set
                duration_seen = least(userHistory.duration_seen + excluded.duration_seen,
                                      (select runtime_minutes from selected)),
                last_seen = excluded.last_seen
            returning title_id, duration_seen = (select runtime_minutes from selected);
        """);

        //SELECT COUNT(DISTINCT title_id) as title_id_distinct_count, COUNT(DISTINCT user_id) as user_id_distinct_count FROM userList;
        // Result:
        //title_id_distinct_count | user_id_distinct_count
        //-------------------------+------------------------
        //                 3725553 |                 100000
        //create index on userList using btree(title_id, user_id);

        //select * from userlist where user_id ='29120' and title_id='tt0505891';
        this.removeTitleFromWatchList = c.prepareStatement("""
            delete from userList
            where user_id = ?
                and title_id = ?
        """);

        /* rateTitle */
        //create index on userHistory using hash(user_id); prefere o btree(user_id, title_id)
        //SELECT COUNT(DISTINCT title_id) as title_id_distinct_count, COUNT(DISTINCT user_id) as user_id_distinct_count FROM userHistory;
        // Result: rever esta parte
        //title_id_distinct_count | user_id_distinct_count
        //-------------------------+------------------------
        //                 4230816 |                 100000
        //create index on userHistory using btree(title_id, user_id);
        //user_id = '100000' and rating = '5'
        this.rateTitleFromHistory = c.prepareStatement("""
            with selected as (
                select user_id, title_id
                from userHistory
                where user_id = ?
                order by random()
                limit 1
            )
            update userHistory
            set rating = ?
            from selected
            where userHistory.user_id = selected.user_id
            and userHistory.title_id = selected.title_id
            returning selected.title_id;
        """);

        //create index on userHistory using hash(title_id); prefere o btree(user_id, title_id)
        //title_id = 'tt7973978'
        this.getTitleRating = c.prepareStatement("""
            select avg(rating)
            from userHistory
            where title_id = ?
        """);

        /* searchTitles */
        //create materialized view titleBetween1980And2023 as select id, title_type, primary_title from title where start_year between 1980 and 2023;
        //? = 'north'
        //this.searchTitleByName = c.prepareStatement("""
        //    select id, title_type, primary_title
        //    from title
        //    where to_tsvector('english', primary_title) @@ to_tsquery('english', ?)
        //        and start_year between 1980 and 2023
        //    limit 20;
        //""");
        //new query
        this.searchTitleByName = c.prepareStatement("""
            select id, title_type, primary_title
            from titleBetween1980And2023
            where to_tsvector('english', primary_title) @@ to_tsquery('english', ?)
            limit 20;
        """);
    }


    /** Adds a new title to the user's watch list */
    public void addNewTitleToList(int userId, Utils.NewTitleOption option, String ... args) throws SQLException {
        var rs = switch (option) {
            // add a title based on the user's genre preference
            case FromPreference -> {
                this.getTitleFromPreference.setInt(1, userId);
                this.getTitleFromPreference.setInt(2, userId);
                yield this.getTitleFromPreference.executeQuery();
            }
            // add a recent title from a given type
            case FromType -> {
                this.getTitleFromType.setString(1, args[0]);
                this.getTitleFromType.setInt(2, userId);
                yield this.getTitleFromType.executeQuery();
            }
            // add a title based on what's currently popular in this last week
            case FromPopular -> {
                this.getTitleFromPopular.setInt(1, userId);
                yield this.getTitleFromPopular.executeQuery();
            }
        };
        if (rs.next()){
            String titleId = rs.getString(1); //error here
            
            this.addTitleToUserList.setInt(1, userId);
            this.addTitleToUserList.setString(2, titleId);
            this.addTitleToUserList.executeUpdate();
            this.conn.commit();
        }
    }


    /** Obtains information about the titles in the user's watch list */
    public List<TitleInfo> getWatchListInformation(int userId) throws SQLException {
        var results = new ArrayList<TitleInfo>();
        this.getUserWatchList.setInt(1, userId);
        var rs = this.getUserWatchList.executeQuery();

        while (rs.next()) {
            var titleInfo = new TitleInfo();
            String titleId = rs.getString(1);

            // info
            this.getTitleInfo.setString(1, titleId);
            var infoRs = this.getTitleInfo.executeQuery();
            infoRs.next();
            titleInfo.titleName = infoRs.getString(1);
            titleInfo.titleType = infoRs.getString(2);
            titleInfo.runtime = infoRs.getInt(3);
            titleInfo.startYear = infoRs.getInt(4);

            // people
            titleInfo.castAndCrew = new HashMap<>(); //retira ordering de getTitleMainCastAndCrew uma vez que não é utilizado
            this.getTitleMainCastAndCrew.setString(1, titleId);
            var peopleRs = this.getTitleMainCastAndCrew.executeQuery();
            while (peopleRs.next()) {
                String personId = peopleRs.getString(1);
                if (!titleInfo.castAndCrew.containsKey(personId)) {
                    var p = new Person();
                    p.name = peopleRs.getString(2);
                    p.job = peopleRs.getString(3);
                    p.characters = new ArrayList<>();
                    titleInfo.castAndCrew.put(personId, p);
                }
                titleInfo.castAndCrew.get(personId).characters.add(peopleRs.getString(4));
            }

            // title in region
            this.getUserRegion.setInt(1, userId);
            var regionRs = this.getUserRegion.executeQuery();
            regionRs.next();
            String userRegion = regionRs.getString(1);
            this.getTitleNameInRegion.setString(1, titleId);
            this.getTitleNameInRegion.setString(2, userRegion);
            regionRs = this.getTitleNameInRegion.executeQuery();
            if (regionRs.next()) {
                titleInfo.titleUserRegion = regionRs.getString(1);
            }
            results.add(titleInfo);
        }

        conn.commit();
        return results;
    }


    /** Inserts (or updates) the history information of a title in the user's watch list */
    public void viewTitle(int userId, int runtime) throws SQLException {
        this.addTitleToHistory.setInt(1, userId);
        this.addTitleToHistory.setInt(2, runtime);
        var rs = this.addTitleToHistory.executeQuery();

        if (rs.next()) {
            String titleId = rs.getString(1);
            boolean finished = rs.getBoolean(2);

            // title has been watched to completion, remove it from the list
            if (finished) {
                this.removeTitleFromWatchList.setInt(1, userId);
                this.removeTitleFromWatchList.setString(2, titleId);
                this.removeTitleFromWatchList.executeUpdate();
            }
        }
        else {
            // should not happen
            System.out.println("No titles found");
        }

        conn.commit();
    }


    /** Rates a title in the user history and returns the title's overall rating */
    public double rateTitle(int userId, int rating) throws SQLException {
        this.rateTitleFromHistory.setInt(1, userId);
        this.rateTitleFromHistory.setInt(2, rating);
        var rs = this.rateTitleFromHistory.executeQuery();
        rs.next();
        String titleId = rs.getString(1);

        this.getTitleRating.setString(1, titleId);
        rs = this.getTitleRating.executeQuery();
        rs.next();
        double newRating = rs.getDouble(1);

        conn.commit();
        return newRating;
    }


    /** Obtains at most 20 titles whose primary_title matches the given search, created between 1980 and 2023 */
    public List<Title> searchTitles(String search) throws SQLException {
        var results = new ArrayList<Title>();
        this.searchTitleByName.setString(1, search);
        var rs = this.searchTitleByName.executeQuery();

        while (rs.next()) {
            var t = new Title();
            t.id = rs.getString(1);
            t.type = rs.getString(2);
            t.name = rs.getString(3);
            results.add(t);
        }

        conn.commit();
        return results;
    }


    public Utils.TransactionType transaction() throws Exception {
        int r = this.rand.nextInt(0, 100);
        int userId = this.rand.nextInt(1, this.nUsers + 1);

        // addNewTitleToList
        if (r < 25) { // 25%
            switch (this.rand.nextInt(20)) {
                case 0 -> addNewTitleToList(userId, Utils.NewTitleOption.FromType,
                        this.titleTypes.get(rand.nextInt(this.titleTypes.size())));
                case 1 -> addNewTitleToList(userId, Utils.NewTitleOption.FromPopular);
                default -> addNewTitleToList(userId, Utils.NewTitleOption.FromPreference);
            }
            return Utils.TransactionType.AddNewTitleToList;
        }
        // getWatchListInformation
        else if (r >= 25 && r < 45) { // 20%
            getWatchListInformation(userId);
            return Utils.TransactionType.GetWatchListInformation;
        }
        // viewTitle
        else if (r >= 45 && r < 85) { // 40%
            int runtime = this.rand.nextInt(20, 60);
            viewTitle(userId, runtime);
            return Utils.TransactionType.ViewTitle;
        }
        // rateTitle
        else if (r >= 85 && r < 88) { // 3%
            int rating = this.rand.nextInt(1, 6);
            rateTitle(userId, rating);
            return Utils.TransactionType.RateTitle;
        }
        // searchTitles
        else { // 12%
            String search = this.searchWords.get(this.rand.nextInt(this.searchWords.size()));
            searchTitles(search);
            return Utils.TransactionType.SearchTitles;
        }
    }
}
