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

        this.getUserWatchList = c.prepareStatement("""
            select title_id, created_date
            from userList
            where user_id = ?
            order by created_date desc
        """);

        this.getTitleInfo = c.prepareStatement("""
            select primary_title, title_type, coalesce(runtime_minutes, 60), start_year
            from title
            where id = ?
        """);

        this.getTitleMainCastAndCrew = c.prepareStatement("""
            select ordering, n.id, n.primary_name, c.name, pc.name
            from titlePrincipals p
            join name n on n.id = p.name_id
            join category c on c.id = p.category_id
            left join titlePrincipalsCharacters pc on pc.title_id = p.title_id and pc.name_id = p.name_id
            where p.title_id = ?
            order by ordering;
        """);

        this.getUserRegion = c.prepareStatement("""
            select country_code
            from users
            where id = ?
        """);

        this.getTitleNameInRegion = c.prepareStatement("""
            select title
            from titleAkas
            where title_id = ?
                and region = ?
        """);

        /* viewTitle */

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

        this.removeTitleFromWatchList = c.prepareStatement("""
            delete from userList
            where user_id = ?
                and title_id = ?
        """);

        /* rateTitle */

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

        this.getTitleRating = c.prepareStatement("""
            select avg(rating)
            from userHistory
            where title_id = ?
        """);

        /* searchTitles */

        this.searchTitleByName = c.prepareStatement("""
            select id, title_type, primary_title
            from title
            where to_tsvector('english', primary_title) @@ to_tsquery('english', ?)
                and start_year between 1980 and 2023
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
        rs.next();
        String titleId = rs.getString(1);

        this.addTitleToUserList.setInt(1, userId);
        this.addTitleToUserList.setString(2, titleId);
        this.addTitleToUserList.executeUpdate();
        this.conn.commit();
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
            titleInfo.castAndCrew = new HashMap<>();
            this.getTitleMainCastAndCrew.setString(1, titleId);
            var peopleRs = this.getTitleMainCastAndCrew.executeQuery();
            while (peopleRs.next()) {
                String personId = peopleRs.getString(2);
                if (!titleInfo.castAndCrew.containsKey(personId)) {
                    var p = new Person();
                    p.name = peopleRs.getString(3);
                    p.job = peopleRs.getString(4);
                    p.characters = new ArrayList<>();
                    titleInfo.castAndCrew.put(personId, p);
                }
                titleInfo.castAndCrew.get(personId).characters.add(peopleRs.getString(5));
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
