import SearchRankingDomainModel._

final case class NonPersonalizedSearchResult(user: UserId, items: List[ItemId])
final case class PersonalizedSearchResult(rankedtems: List[ItemId])

object SearchRankingDomainModel {
  type ItemId = Int
  type UserId = Int
}