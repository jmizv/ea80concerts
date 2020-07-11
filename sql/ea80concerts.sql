drop view if exists v_concert;
drop table if exists concert;
drop table if exists venue;
drop table if exists city;
drop table if exists band;

create table city
(
	id bigint primary key,
	name varchar(255) not null,
	country varchar(2) not null default 'DE'
);
/*
create trigger alterCountry
	AFTER INSERT ON city
	FOR EACH ROW
*/
create table venue
(
	id bigint primary key,
	name varchar(255) not null,
	cityid bigint not null references city(id)
);

create table band
(
	id bigint primary key,
	name varchar(255) not null
);

create table concert
(
	id bigint primary key,
	dateofconcert date not null,
	dateformat varchar(16) not null default 'dd.MM.yyyy',
	venueid bigint not null references venue(id),
	otherbands bigint[],
	canceled boolean not null default '0',
	price_boxoffice numeric(6,2),
	price_preselling numeric(6,2),
	currency varchar(3) default 'EUR'
);

CREATE VIEW v_concert AS
 SELECT dateofconcert, c.name "city", v.name "venue", otherbands, case when canceled='0' then '' else 'abgesagt' end
 FROM concert k,city c,venue v
 WHERE k.venueid=v.id AND v.cityid=c.id
 ORDER by 1 DESC;


CREATE OR REPLACE PROCEDURE createConcert(VARCHAR(10),VARCHAR(255),VARCHAR(255),VARCHAR(255)[],BOOLEAN,NUMERIC(6,2),NUMERIC(6,2))
LANGUAGE plpgsql
AS $$
DECLARE
 vBandId bigint;
 vVenueId bigint;
 vCityId bigint;
 vConcertId bigint;
 vBandsId bigint[];
 vBandName varchar(255);
BEGIN
SELECT max(id) INTO vCityId FROM city WHERE name=$2;
IF vCityId IS NULL THEN
	SELECT coalesce(max(id),0)+1 INTO vCityId FROM city;
	INSERT INTO city (id,name) VALUES (vCityId,$2);
END IF;

SELECT max(id) INTO vVenueId FROM venue WHERE name=$3 AND cityid=vCityId;
IF vVenueId IS NULL THEN
	SELECT coalesce(max(id),0)+1 INTO vVenueId FROM venue;
	INSERT INTO venue(id,name,cityid) VALUES (vVenueId,$3,vCityId);
END IF;

IF $4 IS NOT NULL THEN
	FOREACH vBandName IN ARRAY $4
	LOOP
		SELECT max(id) INTO vBandId FROM band WHERE name=vBandName;
		IF vBandId IS NULL THEN
			SELECT coalesce(max(id),0)+1 INTO vBandId FROM band;
			INSERT INTO band(id,name) VALUES (vBandId,vBandName);
		END IF;
		vBandsId := array_append(vBandsId,vBandId);
	END LOOP;
END IF;

SELECT coalesce(max(id),0)+1 INTO vConcertId FROM concert;
INSERT INTO concert (id,dateofconcert,venueid,otherbands,canceled,price_boxoffice,price_preselling)
	VALUES (vConcertId,cast($1 as date),vVenueId,vBandsId,$5,$6,$7);

END;
$$

/*
CALL createConcert('16.09.2019','Hamburg','Knust',null,'0',null,null);
CALL createConcert('17.05.2019','Esslingen','Komma',ARRAY['The Boiler'],false,10.00,12.00);
CALL createConcert('02.06.2018','Esslingen','Komma',ARRAY['Toylettes'],'1',null,null);
CALL createConcert('25.03.2017','Düsseldorf','Haus der Jugend',ARRAY['Finisterre'],'0',null,null);
CALL createConcert('28.04.2017','Wilhelmshaven','Kling Klang',null,'0',null,null);
CALL createConcert('19.05.2017','Berlin','Tiefgrund',ARRAY['Levitations'],'0',null,null);
CALL createConcert('20.05.2017','Berlin','Tiefgrund',ARRAY['Femme Krawall'],'0',null,null);
CALL createConcert('17.06.2017','Hamburg','MS Stubnitz',ARRAY['Oiro','Boy Division'],'0',null,null);
CALL createConcert('02.10.2017','Krefeld','Kulturfabrik',ARRAY['Die Strafe'],'0',null,null);

CALL createConcert('12.03.2016','Leverkusen','KAW',ARRAY['licht-ung','ZNOUZECTNOST'],'0',null,null);
CALL createConcert('23.04.2016','Gera','Jugendhaus Shalom',ARRAY['Nervöus'],'0',null,null);
CALL createConcert('24.04.2016','Frankfurt','AU',null,'0',null,null);
CALL createConcert('22.05.2016','Düsseldorf','AK47',null,'0',null,null);
CALL createConcert('01.07.2016','Nürnberg','Komm',ARRAY['Klotzs'],'1',null,null); -- Fällt aus !!!
CALL createConcert('02.07.2016','München','Cafe Kult',ARRAY['Klotzs'],'0',null,null);
CALL createConcert('03.07.2016','Gießen','AK44',ARRAY['Klotzs'],'0',null,null);
CALL createConcert('18.11.2016','Nürnberg','K4',ARRAY['Klotzs'],'0',null,null);
CALL createConcert('19.11.2016','Esslingen','Komma',ARRAY['Klotzs'],'0',null,null);
CALL createConcert('17.12.2016','Wuppertal','Die Börse',ARRAY['Pisse'],'0',null,null);



SELECT to_char(dateofconcert,'DD.MM.YYYY'),city,venue,otherbands,case when canceled='0' then '' else 'abgesagt' end
FROM concert ORDER by dateofconcert DESC


*/