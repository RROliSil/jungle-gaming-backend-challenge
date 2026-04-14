import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Inject,
  Injectable,
  SetMetadata,
  UnauthorizedException,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { createRemoteJWKSet, jwtVerify } from 'jose';
import {
  AuthenticatedPrincipal,
  PROVIDER_IDENTITY_PORT,
  ProviderIdentityPort,
} from '../../application/ports/application.ports';

export const IS_PUBLIC_KEY = 'isPublicEndpoint';
export const PublicEndpoint = () => SetMetadata(IS_PUBLIC_KEY, true);

/**
 * External OIDC Identity Provider adapter (Keycloak / Zitadel - Section 2).
 * Uses OIDC JWKS endpoint to verify RS256/ES256 tokens issued by the external IdP,
 * avoiding any custom user/password tables in the wagering database.
 */
@Injectable()
export class KeycloakOidcIdentityAdapter implements ProviderIdentityPort {
  private jwks?: ReturnType<typeof createRemoteJWKSet>;

  private getJwks() {
    if (!this.jwks) {
      const jwksUri =
        process.env.OIDC_JWKS_URI ??
        'http://localhost:8080/realms/jungle-gaming/protocol/openid-connect/certs';
      this.jwks = createRemoteJWKSet(new URL(jwksUri));
    }
    return this.jwks;
  }

  async verifyToken(bearerToken: string): Promise<AuthenticatedPrincipal> {
    try {
      const issuer = process.env.OIDC_ISSUER_URL;
      const audience = process.env.OIDC_AUDIENCE;

      const { payload } = await jwtVerify(bearerToken, this.getJwks(), {
        issuer: issuer || undefined,
        audience: audience || undefined,
      });

      const realmAccess = payload.realm_access as { roles?: string[] } | undefined;
      const roles = Array.isArray(realmAccess?.roles) ? realmAccess.roles : [];

      return {
        subject: String(payload.sub ?? 'unknown-subject'),
        providerId:
          (payload.provider_id as string | undefined) ??
          (payload.azp as string | undefined),
        roles,
        issuer: String(payload.iss ?? ''),
        claims: payload as Record<string, unknown>,
      };
    } catch (err: unknown) {
      throw new UnauthorizedException({
        code: 'INVALID_OIDC_TOKEN',
        message: `OIDC token verification failed: ${(err as Error)?.message ?? 'unauthorized'}`,
      });
    }
  }

  assertProviderAccess(
    principal: AuthenticatedPrincipal,
    providerId: string,
  ): void {
    if (principal.roles.includes('admin') || principal.roles.includes('system')) {
      return;
    }
    if (principal.providerId && principal.providerId !== providerId) {
      throw new ForbiddenException({
        code: 'PROVIDER_IDENTITY_MISMATCH',
        message: `Token principal (${principal.providerId}) is not authorized to act as provider '${providerId}'`,
      });
    }
  }
}

@Injectable()
export class OidcAuthGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    @Inject(PROVIDER_IDENTITY_PORT)
    private readonly identityPort: ProviderIdentityPort,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC_KEY, [
      context.getHandler(),
      context.getClass(),
    ]);

    if (isPublic) {
      return true;
    }

    const request = context.switchToHttp().getRequest<{
      url?: string;
      headers?: Record<string, string | undefined>;
      body?: Record<string, unknown>;
      user?: AuthenticatedPrincipal;
    }>();

    // Section 2 & 9: Health endpoints are always open without authentication
    if (request.url?.startsWith('/health')) {
      return true;
    }

    if (process.env.AUTH_ENABLED !== 'true') {
      request.user = {
        subject: 'passthrough-dev-principal',
        providerId: (request.body?.providerId as string | undefined) ?? undefined,
        roles: ['system'],
        issuer: 'passthrough',
        claims: {},
      };
      return true;
    }

    const authHeader =
      request.headers?.authorization ?? request.headers?.Authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      throw new UnauthorizedException({
        code: 'MISSING_BEARER_TOKEN',
        message: 'Authorization Bearer token from external Identity Provider is required',
      });
    }

    const token = authHeader.slice('Bearer '.length).trim();
    const principal = await this.identityPort.verifyToken(token);
    request.user = principal;

    const bodyProviderId = request.body?.providerId as string | undefined;
    if (bodyProviderId) {
      this.identityPort.assertProviderAccess(principal, bodyProviderId);
    }

    return true;
  }
}
