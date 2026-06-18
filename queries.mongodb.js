/*
R
*/

// 1. Top 5 most frequently occurring comments

db.processed_reviews.aggregate([
  {
    $group: {
      _id: "$content",
      count: { $sum: 1 }
    }
  },
  {
    $sort: { count: -1 }
  },
  {
    $limit: 5
  }
]);

// 2. All entries where the content field is fewer than 5 characters

db.processed_reviews.aggregate([
  {
    $project: {
      content: 1,
      score: 1,
      at: 1,
      length: { $strLenCP: "$content" }
    }
  },
  {
    $match: {
      length: { $lt: 5 }
    }
  }
]);

// 3. Average rating for each day (result in timestamp type)

db.processed_reviews.aggregate([
  {
    $match: {
      at: { $type: "date" }
    }
  },
  {
    $group: {
      _id: {
        $dateTrunc: { date: "$at", unit: "day" }
      },
      avg_rating: { $avg: "$score" }
    }
  },
  {
    $project: {
      day: "$_id",
      avg_rating: { $round: ["$avg_rating", 2] }
    }
  },
  {
    $sort: { day: 1 }
  }
]);
