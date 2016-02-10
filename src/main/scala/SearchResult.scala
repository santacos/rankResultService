import IdType.{UserId, ItemId}

/**
  * Created by ibosz on 10/2/59.
  */
trait SearchResult

final case class NonPersonalizedSearchResult(user: UserId, items: List[ItemId]) extends SearchResult
final case class PersonalizedSearchResult(items: List[ItemId]) extends SearchResult