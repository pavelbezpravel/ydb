#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IRequestNoOpCtx;
class IFacilityProvider;

namespace NEtcd {

void DoRange(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoPut(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoDeleteRange(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&);
void DoTxn(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoCompact(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);

} // namespace NEtcd

} // namespace NKikimr::NGRpcService
